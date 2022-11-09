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

package org.apache.flink.runtime.io.network.partition.store.tier.dfs;

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
import org.apache.flink.runtime.io.network.partition.store.TieredStoreMode;
import org.apache.flink.runtime.io.network.partition.store.common.BufferContext;
import org.apache.flink.runtime.io.network.partition.store.common.BufferIndexAndChannel;
import org.apache.flink.runtime.io.network.partition.store.common.BufferPoolHelper;
import org.apache.flink.runtime.io.network.partition.store.common.BufferWithIdentity;
import org.apache.flink.runtime.io.network.partition.store.common.CacheDataSpiller;
import org.apache.flink.runtime.io.network.partition.store.common.ConsumerId;
import org.apache.flink.runtime.io.network.partition.store.tier.local.file.CacheDataManager;
import org.apache.flink.runtime.io.network.partition.store.tier.local.file.OutputMetrics;
import org.apache.flink.runtime.io.network.partition.store.tier.local.file.RegionBufferIndexTracker;
import org.apache.flink.runtime.io.network.partition.store.tier.local.file.SubpartitionConsumerCacheDataManager;
import org.apache.flink.util.function.SupplierWithException;
import org.apache.flink.util.function.ThrowingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * This class is responsible for managing the data in a single subpartition. One {@link
 * CacheDataManager} will hold multiple {@link SubpartitionConsumerCacheDataManager}.
 */
public class SubpartitionDfsCacheDataManager {

    private static final Logger LOG =
            LoggerFactory.getLogger(SubpartitionDfsCacheDataManager.class);

    private final int targetChannel;

    private final int bufferSize;

    private static final int NUM_BUFFERS_TO_FLUSH = 1;

    private final DfsCacheDataManagerOperation cacheDataManagerOperation;

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

    private final CacheDataSpiller cacheDataSpiller;

    private final BufferPoolHelper bufferPoolHelper;

    /** DO NOT USE DIRECTLY. Use {@link #runWithLock} or {@link #callWithLock} instead. */
    private final Lock resultPartitionLock;

    /** DO NOT USE DIRECTLY. Use {@link #runWithLock} or {@link #callWithLock} instead. */
    private final ReentrantReadWriteLock subpartitionLock = new ReentrantReadWriteLock();

    @GuardedBy("subpartitionLock")
    private final Map<ConsumerId, SubpartitionDfsConsumerCacheDataManager> consumerMap;

    @Nullable private final BufferCompressor bufferCompressor;

    @Nullable private OutputMetrics outputMetrics;

    private volatile boolean isClosed;

    private volatile boolean isReleased;

    private final boolean isBroadcastOnly;

    public SubpartitionDfsCacheDataManager(
            JobID jobID,
            ResultPartitionID resultPartitionID,
            int targetChannel,
            int bufferSize,
            boolean isBroadcastOnly,
            BufferPoolHelper bufferPoolHelper,
            String baseDfsPath,
            Lock resultPartitionLock,
            @Nullable BufferCompressor bufferCompressor,
            DfsCacheDataManagerOperation cacheDataManagerOperation,
            ExecutorService ioExecutor)
            throws IOException {
        this.targetChannel = targetChannel;
        this.bufferSize = bufferSize;
        this.bufferPoolHelper = bufferPoolHelper;
        this.resultPartitionLock = resultPartitionLock;
        this.cacheDataManagerOperation = cacheDataManagerOperation;
        this.bufferCompressor = bufferCompressor;
        this.consumerMap = new HashMap<>();
        this.cacheDataSpiller =
                new CacheDataDfsFileSpiller(
                        jobID, resultPartitionID, targetChannel, baseDfsPath, ioExecutor);
        bufferPoolHelper.registerSubpartitionTieredManager(
                targetChannel, TieredStoreMode.TieredType.IN_DFS, this::flushCachedBuffers);
        this.isBroadcastOnly = isBroadcastOnly;
    }

    // ------------------------------------------------------------------------
    //  Called by DfsCacheDataManager
    // ------------------------------------------------------------------------

    /**
     * Append record to {@link SubpartitionConsumerCacheDataManager}.
     *
     * @param record to be managed by this class.
     * @param dataType the type of this record. In other words, is it data or event.
     * @param isLastRecordInSegment whether this record is the last record in a segment.
     */
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

    public void startSegment(long segmentIndex) throws IOException {
        cacheDataSpiller.startSegment(segmentIndex);
    }

    public void finishSegment(long segmentIndex) {
        List<ConsumerId> needNotify = new ArrayList<>(consumerMap.size());
        runWithLock(
                () -> {
                    LOG.debug("%%% Dfs generate1");
                    CompletableFuture<List<RegionBufferIndexTracker.SpilledBuffer>>
                            spillDoneFuture = flushCachedBuffers();
                    try {
                        spillDoneFuture.get();
                    } catch (Exception e) {
                        throw new RuntimeException("Spiller finish segment failed!", e);
                    }
                    cacheDataSpiller.finishSegment(segmentIndex);
                    LOG.debug("%%% Dfs generate2");
                    BufferContext segmentInfoBufferContext =
                            new BufferContext(null, finishedSegmentInfoIndex, targetChannel, true, true);
                    allSegmentInfos.add(segmentInfoBufferContext);
                    ++finishedSegmentInfoIndex;
                    checkState(allBuffers.isEmpty(), "Leaking finished buffers.");
                    LOG.debug("%%% Dfs generate3 {}", consumerMap.entrySet().size());
                    // notify downstream
                    for (Map.Entry<ConsumerId, SubpartitionDfsConsumerCacheDataManager>
                            consumerEntry : consumerMap.entrySet()) {
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
        }
    }

    public void releaseConsumer(ConsumerId consumerId) {
        runWithLock(() -> checkNotNull(consumerMap.remove(consumerId)));
    }

    @SuppressWarnings("FieldAccessNotGuarded")
    public SubpartitionDfsConsumerCacheDataManager registerNewConsumer(ConsumerId consumerId) {
        return callWithLock(
                () -> {
                    checkState(!consumerMap.containsKey(consumerId));
                    SubpartitionDfsConsumerCacheDataManager newConsumer =
                            new SubpartitionDfsConsumerCacheDataManager(
                                    resultPartitionLock,
                                    subpartitionLock.readLock(),
                                    targetChannel,
                                    consumerId,
                                    cacheDataManagerOperation);
                    newConsumer.addInitialBuffers(allSegmentInfos);
                    consumerMap.put(consumerId, newConsumer);
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
            LOG.debug("%%% Dfs write record1");
            BufferBuilder currentWritingBuffer =
                    checkNotNull(
                            unfinishedBuffers.peek(), "Expect enough capacity for the record.");
            currentWritingBuffer.append(record);
            LOG.debug("%%% Dfs write record2");
            if (currentWritingBuffer.isFull() && record.hasRemaining()) {
                LOG.debug("%%% Dfs write record3");
                finishCurrentWritingBuffer(false);
            } else if (currentWritingBuffer.isFull() && !record.hasRemaining()) {
                LOG.debug("%%% Dfs write record4");
                finishCurrentWritingBuffer(isLastRecordInSegment);
            } else if (!currentWritingBuffer.isFull() && !record.hasRemaining()) {
                LOG.debug("%%% Dfs write record4.1");
                if (isLastRecordInSegment) {
                    LOG.debug("%%% Dfs write record5");
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
                bufferPoolHelper.requestMemorySegmentBlocking(
                        targetChannel, TieredStoreMode.TieredType.IN_DFS, false);
        return new BufferBuilder(segment, this::recycleBuffer);
    }

    private void recycleBuffer(MemorySegment buffer) {
        bufferPoolHelper.recycleBuffer(
                targetChannel, buffer, TieredStoreMode.TieredType.IN_DFS, false);
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

    /**
     * Remove all released buffer from head of queue until buffer queue is empty or meet un-released
     * buffer.
     */
    @GuardedBy("subpartitionLock")
    private void trimHeadingReleasedBuffers(Deque<BufferContext> bufferQueue) {
        while (!bufferQueue.isEmpty() && bufferQueue.peekFirst().isReleased()) {
            bufferQueue.removeFirst();
        }
    }

    @GuardedBy("subpartitionLock")
    private Optional<BufferContext> startSpillingBuffer(
            int bufferIndex, CompletableFuture<Void> spillFuture) {
        BufferContext bufferContext = bufferIndexToContexts.get(bufferIndex);
        if (bufferContext == null) {
            return Optional.empty();
        }
        return bufferContext.startSpilling(spillFuture)
                ? Optional.of(bufferContext)
                : Optional.empty();
    }

    private CompletableFuture<List<RegionBufferIndexTracker.SpilledBuffer>> flushCachedBuffers() {
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

        List<BufferWithIdentity> toSpillBuffersWithId = getStSpillBuffersWithId(toSpillBuffers);
        return cacheDataSpiller.spillAsync(toSpillBuffersWithId);
    }

    private List<BufferWithIdentity> getStSpillBuffersWithId(
            List<BufferIndexAndChannel> toSpillBuffers) {
        List<BufferWithIdentity> toSpillBuffersWithId = new ArrayList<>();
        for (BufferIndexAndChannel spillBuffer : toSpillBuffers) {
            int bufferIndex = spillBuffer.getBufferIndex();
            Optional<BufferContext> bufferContext =
                    startSpillingBuffer(bufferIndex, CompletableFuture.completedFuture(null));
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
        try {
            flushCachedBuffers().get();
        } catch (Exception e) {
            throw new RuntimeException(
                    "Flush data to dfs failed when DfsFileWriter is trying to close.", e);
        }
        while (!unfinishedBuffers.isEmpty()) {
            unfinishedBuffers.poll().close();
        }
    }

    private <E extends Exception> void runWithLock(ThrowingRunnable<E> runnable) throws E {
        try {
            resultPartitionLock.lock();
            subpartitionLock.writeLock().lock();
            runnable.run();
        } finally {
            subpartitionLock.writeLock().unlock();
            resultPartitionLock.unlock();
        }
    }

    private <R, E extends Exception> R callWithLock(SupplierWithException<R, E> callable) throws E {
        try {
            resultPartitionLock.lock();
            subpartitionLock.writeLock().lock();
            return callable.get();
        } finally {
            subpartitionLock.writeLock().unlock();
            resultPartitionLock.unlock();
        }
    }

    @VisibleForTesting
    public Path getBaseSubpartitionPath() {
        return cacheDataSpiller.getBaseSubpartitionPath();
    }
}
