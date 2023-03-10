/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.remote;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.TieredStoreMode;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.BufferContext;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.BufferIndexAndChannel;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.BufferWithIdentity;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.CacheBufferSpiller;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.CacheFlushManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierReaderViewId;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreMemoryManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.OutputMetrics;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.RegionBufferIndexTracker;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.SupplierWithException;
import org.apache.flink.util.function.ThrowingRunnable;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreUtils.checkFlushCacheBuffers;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** This class is responsible for managing the data in a single subpartition. */
public class SubpartitionRemoteCacheManager {

    private final int targetChannel;

    private final int bufferSize;

    private static final int NUM_BUFFERS_TO_FLUSH = 1;

    private final RemoteCacheManagerOperation cacheDataManagerOperation;

    // Not guarded by lock because it is expected only accessed from task's main thread.
    private final Queue<BufferBuilder> unfinishedBuffers = new LinkedList<>();

    // Not guarded by lock because it is expected only accessed from task's main thread.
    private int finishedBufferIndex;

    // Not guarded by lock because it is expected only accessed from task's main thread.
    private int finishedSegmentInfoIndex;

    @GuardedBy("subpartitionLock")
    private final Deque<BufferContext> allSegmentInfos = new LinkedList<>();

    @GuardedBy("subpartitionLock")
    private final Deque<BufferContext> allBuffers = new LinkedList<>();

    @GuardedBy("subpartitionLock")
    private final Map<Integer, BufferContext> bufferIndexToContexts = new HashMap<>();

    private final CacheBufferSpiller cacheBufferSpiller;

    private final TieredStoreMemoryManager tieredStoreMemoryManager;

    private final ReentrantReadWriteLock subpartitionLock = new ReentrantReadWriteLock();

    @GuardedBy("subpartitionLock")
    private final Map<TierReaderViewId, RemoteTierReader> consumerMap;

    @Nullable private final BufferCompressor bufferCompressor;

    @Nullable private OutputMetrics outputMetrics;

    private boolean isSegmentStarted = false;

    private volatile boolean isClosed;

    private volatile boolean isReleased;

    private CompletableFuture<Void> hasFlushCompleted = FutureUtils.completedVoidFuture();

    public SubpartitionRemoteCacheManager(
            JobID jobID,
            ResultPartitionID resultPartitionID,
            int targetChannel,
            int bufferSize,
            TieredStoreMemoryManager tieredStoreMemoryManager,
            CacheFlushManager cacheFlushManager,
            String baseDfsPath,
            @Nullable BufferCompressor bufferCompressor,
            RemoteCacheManagerOperation cacheDataManagerOperation,
            ExecutorService ioExecutor) {
        this.targetChannel = targetChannel;
        this.bufferSize = bufferSize;
        this.tieredStoreMemoryManager = tieredStoreMemoryManager;
        this.cacheDataManagerOperation = cacheDataManagerOperation;
        this.bufferCompressor = bufferCompressor;
        this.consumerMap = new HashMap<>();
        this.cacheBufferSpiller =
                new RemoteCacheBufferSpiller(
                        jobID, resultPartitionID, targetChannel, baseDfsPath, ioExecutor);
        cacheFlushManager.registerCacheSpillTrigger(this::flushCachedBuffers);
    }

    // ------------------------------------------------------------------------
    //  Called by DfsCacheDataManager
    // ------------------------------------------------------------------------

    public void append(ByteBuffer record, Buffer.DataType dataType, boolean isLastRecordInSegment)
            throws InterruptedException {
        if (dataType.isEvent()) {
            writeEvent(record, dataType, isLastRecordInSegment);
        } else {
            writeRecord(record, dataType, isLastRecordInSegment);
        }
    }

    public void setOutputMetrics(OutputMetrics outputMetrics) {
        this.outputMetrics = checkNotNull(outputMetrics);
    }

    public void startSegment(int segmentIndex) throws IOException {
        checkState(!isSegmentStarted);
        isSegmentStarted = true;
        cacheBufferSpiller.startSegment(segmentIndex);
    }

    public void finishSegment(int segmentIndex) {
        checkState(isSegmentStarted);
        isSegmentStarted = false;
        List<TierReaderViewId> needNotify = new ArrayList<>(consumerMap.size());
        runWithLock(
                () -> {
                    CompletableFuture<List<RegionBufferIndexTracker.SpilledBuffer>>
                            spillDoneFuture = flushCachedBuffers();
                    try {
                        spillDoneFuture.get();
                        checkFlushCacheBuffers(tieredStoreMemoryManager, this::flushCachedBuffers);
                    } catch (Exception e) {
                        throw new RuntimeException("Spiller finish segment failed!", e);
                    }
                    cacheBufferSpiller.finishSegment(segmentIndex);
                    BufferContext segmentInfoBufferContext =
                            new BufferContext(null, finishedSegmentInfoIndex, targetChannel, true);
                    allSegmentInfos.add(segmentInfoBufferContext);
                    ++finishedSegmentInfoIndex;
                    checkState(allBuffers.isEmpty(), "Leaking finished buffers.");
                    // notify downstream
                    for (Map.Entry<TierReaderViewId, RemoteTierReader> consumerEntry :
                            consumerMap.entrySet()) {
                        if (consumerEntry.getValue().addBuffer(segmentInfoBufferContext)) {
                            needNotify.add(consumerEntry.getKey());
                        }
                    }
                });
        cacheDataManagerOperation.onDataAvailable(targetChannel, needNotify);
    }

    /** Release all buffers. */
    public void release() {
        if (!isReleased) {
            for (BufferContext bufferContext : allBuffers) {
                bufferContext.release();
            }
            allBuffers.clear();
            bufferIndexToContexts.clear();
            isReleased = true;
            hasFlushCompleted.complete(null);
        }
    }

    public void releaseConsumer(TierReaderViewId tierReaderViewId) {
        runWithLock(() -> checkNotNull(consumerMap.remove(tierReaderViewId)));
    }

    @SuppressWarnings("FieldAccessNotGuarded")
    public RemoteTierReader registerNewConsumer(TierReaderViewId tierReaderViewId) {
        return callWithLock(
                () -> {
                    checkState(!consumerMap.containsKey(tierReaderViewId));
                    RemoteTierReader newConsumer =
                            new RemoteTierReader(
                                    subpartitionLock.readLock(),
                                    targetChannel,
                                    tierReaderViewId,
                                    cacheDataManagerOperation);
                    newConsumer.addInitialBuffers(allSegmentInfos);
                    consumerMap.put(tierReaderViewId, newConsumer);
                    return newConsumer;
                });
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    private void writeEvent(
            ByteBuffer event, Buffer.DataType dataType, boolean isLastRecordInSegment) {
        checkArgument(dataType.isEvent());

        // each Event must take an exclusive buffer
        finishCurrentWritingBufferIfNotEmpty();

        // store Events in adhoc heap segments, for network memory efficiency
        MemorySegment data = MemorySegmentFactory.wrap(event.array());
        Buffer buffer =
                new NetworkBuffer(data, FreeingBufferRecycler.INSTANCE, dataType, data.size());

        BufferContext bufferContext =
                new BufferContext(
                        buffer, finishedBufferIndex, targetChannel, isLastRecordInSegment);
        addFinishedBuffer(bufferContext);
    }

    private void writeRecord(
            ByteBuffer record, Buffer.DataType dataType, boolean isLastRecordInSegment)
            throws InterruptedException {
        checkArgument(!dataType.isEvent());

        ensureCapacityForRecord(record);

        writeRecord(record, isLastRecordInSegment);
    }

    private void ensureCapacityForRecord(ByteBuffer record) throws InterruptedException {
        final int numRecordBytes = record.remaining();
        int availableBytes =
                Optional.ofNullable(unfinishedBuffers.peek())
                        .map(
                                currentWritingBuffer ->
                                        currentWritingBuffer.getWritableBytes()
                                                + bufferSize * (unfinishedBuffers.size() - 1))
                        .orElse(0);

        while (availableBytes < numRecordBytes) {
            // request unfinished buffer.
            BufferBuilder bufferBuilder = requestBufferFromPool();
            unfinishedBuffers.add(bufferBuilder);
            availableBytes += bufferSize;
        }
    }

    private void writeRecord(ByteBuffer record, boolean isLastRecordInSegment) {
        while (record.hasRemaining()) {
            BufferBuilder currentWritingBuffer =
                    checkNotNull(
                            unfinishedBuffers.peek(), "Expect enough capacity for the record.");
            currentWritingBuffer.append(record);
            if (currentWritingBuffer.isFull() && record.hasRemaining()) {
                finishCurrentWritingBuffer(false);
            } else if (currentWritingBuffer.isFull() && !record.hasRemaining()) {
                finishCurrentWritingBuffer(isLastRecordInSegment);
            } else if (!currentWritingBuffer.isFull() && !record.hasRemaining()) {
                if (isLastRecordInSegment) {
                    finishCurrentWritingBuffer(true);
                }
            }
        }
    }

    private void finishCurrentWritingBufferIfNotEmpty() {
        BufferBuilder currentWritingBuffer = unfinishedBuffers.peek();
        if (currentWritingBuffer == null || currentWritingBuffer.getWritableBytes() == bufferSize) {
            return;
        }

        finishCurrentWritingBuffer(false);
    }

    private void finishCurrentWritingBuffer(boolean isLastBufferInSegment) {
        BufferBuilder currentWritingBuffer = unfinishedBuffers.poll();

        if (currentWritingBuffer == null || isClosed) {
            return;
        }

        currentWritingBuffer.finish();
        BufferConsumer bufferConsumer = currentWritingBuffer.createBufferConsumerFromBeginning();
        Buffer buffer = bufferConsumer.build();
        currentWritingBuffer.close();
        bufferConsumer.close();
        BufferContext bufferContext =
                new BufferContext(
                        compressBuffersIfPossible(buffer),
                        finishedBufferIndex,
                        targetChannel,
                        isLastBufferInSegment);
        addFinishedBuffer(bufferContext);
    }

    private Buffer compressBuffersIfPossible(Buffer buffer) {
        if (!canBeCompressed(buffer)) {
            return buffer;
        }
        return checkNotNull(bufferCompressor).compressToOriginalBuffer(buffer);
    }

    /**
     * Whether the buffer can be compressed or not. Note that event is not compressed because it is
     * usually small and the size can become even larger after compression.
     */
    private boolean canBeCompressed(Buffer buffer) {
        return bufferCompressor != null && buffer.isBuffer() && buffer.readableBytes() > 0;
    }

    public BufferBuilder requestBufferFromPool() {
        MemorySegment segment =
                tieredStoreMemoryManager.requestMemorySegmentBlocking(
                        TieredStoreMode.TieredType.IN_DFS);
        tryCheckFlushCacheBuffers();
        return new BufferBuilder(segment, this::recycleBuffer);
    }

    private void tryCheckFlushCacheBuffers() {
        if (hasFlushCompleted.isDone()) {
            checkFlushCacheBuffers(
                    tieredStoreMemoryManager, this::flushCachedBuffersWithChangeFlushStatus);
        }
    }

    private void recycleBuffer(MemorySegment buffer) {
        tieredStoreMemoryManager.recycleBuffer(buffer, TieredStoreMode.TieredType.IN_DFS);
    }

    @SuppressWarnings("FieldAccessNotGuarded")
    // Note that: callWithLock ensure that code block guarded by resultPartitionReadLock and
    // subpartitionLock.
    private void addFinishedBuffer(BufferContext bufferContext) {
        runWithLock(
                () -> {
                    finishedBufferIndex++;
                    allBuffers.add(bufferContext);
                    bufferIndexToContexts.put(
                            bufferContext.getBufferIndexAndChannel().getBufferIndex(),
                            bufferContext);
                    updateStatistics(bufferContext.getBuffer());
                    if (allBuffers.size() >= NUM_BUFFERS_TO_FLUSH) {
                        flushCachedBuffers();
                    }
                });
    }

    @GuardedBy("subpartitionLock")
    private Optional<BufferContext> startSpillingBuffer(int bufferIndex) {
        BufferContext bufferContext = bufferIndexToContexts.get(bufferIndex);
        if (bufferContext == null) {
            return Optional.empty();
        }
        return bufferContext.startSpilling() ? Optional.of(bufferContext) : Optional.empty();
    }

    private void flushCachedBuffersWithChangeFlushStatus() {
        List<BufferWithIdentity> toSpillBuffersWithId = generateToSpillBuffersWithId();
        hasFlushCompleted = new CompletableFuture<>();
        cacheBufferSpiller
                .spillAsync(toSpillBuffersWithId)
                .thenApply(
                        spilledBuffers -> {
                            hasFlushCompleted.complete(null);
                            return spilledBuffers;
                        });
    }

    private CompletableFuture<List<RegionBufferIndexTracker.SpilledBuffer>> flushCachedBuffers() {
        return cacheBufferSpiller.spillAsync(generateToSpillBuffersWithId());
    }

    private List<BufferWithIdentity> generateToSpillBuffersWithId() {
        List<BufferIndexAndChannel> toSpillBuffers =
                callWithLock(
                        () -> {
                            List<BufferIndexAndChannel> targetBuffers = new ArrayList<>();
                            allBuffers.forEach(
                                    (bufferContext ->
                                            targetBuffers.add(
                                                    bufferContext.getBufferIndexAndChannel())));
                            allBuffers.clear();
                            return targetBuffers;
                        });

        return getSpillBuffersWithId(toSpillBuffers);
    }

    private List<BufferWithIdentity> getSpillBuffersWithId(
            List<BufferIndexAndChannel> toSpillBuffers) {
        List<BufferWithIdentity> toSpillBuffersWithId = new ArrayList<>();
        for (BufferIndexAndChannel spillBuffer : toSpillBuffers) {
            int bufferIndex = spillBuffer.getBufferIndex();
            Optional<BufferContext> bufferContext = startSpillingBuffer(bufferIndex);
            checkState(bufferContext.isPresent());
            BufferWithIdentity bufferWithIdentity =
                    new BufferWithIdentity(
                            bufferContext.get().getBuffer(), bufferIndex, targetChannel);
            toSpillBuffersWithId.add(bufferWithIdentity);
        }
        return toSpillBuffersWithId;
    }

    private void updateStatistics(Buffer buffer) {
        checkNotNull(outputMetrics).getNumBuffersOut().inc();
        checkNotNull(outputMetrics).getNumBytesOut().inc(buffer.readableBytes());
    }

    void close() {
        isClosed = true;
        hasFlushCompleted.complete(null);
        flushCachedBuffers();
        while (!unfinishedBuffers.isEmpty()) {
            unfinishedBuffers.poll().close();
        }
    }

    private <E extends Exception> void runWithLock(ThrowingRunnable<E> runnable) throws E {
        try {
            subpartitionLock.writeLock().lock();
            runnable.run();
        } finally {
            subpartitionLock.writeLock().unlock();
        }
    }

    private <R, E extends Exception> R callWithLock(SupplierWithException<R, E> callable) throws E {
        try {
            subpartitionLock.writeLock().lock();
            return callable.get();
        } finally {
            subpartitionLock.writeLock().unlock();
        }
    }

    @VisibleForTesting
    public Path getBaseSubpartitionPath() {
        return cacheBufferSpiller.getBaseSubpartitionPath();
    }
}
