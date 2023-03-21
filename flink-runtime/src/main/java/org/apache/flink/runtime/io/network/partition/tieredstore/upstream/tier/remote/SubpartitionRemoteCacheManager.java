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
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.TieredStoreMode;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.BufferContext;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.CacheFlushManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreMemoryManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.file.PartitionFileWriter;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.OutputMetrics;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreUtils.checkFlushCacheBuffers;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** This class is responsible for managing the data in a single subpartition. */
public class SubpartitionRemoteCacheManager {

    private final int targetChannel;

    private final int bufferSize;

    private static final int NUM_BUFFERS_TO_FLUSH = 1;

    // Not guarded by lock because it is expected only accessed from task's main thread.
    private final Queue<BufferBuilder> unfinishedBuffers = new LinkedList<>();

    // Not guarded by lock because it is expected only accessed from task's main thread.
    private int finishedBufferIndex;

    private final Deque<BufferContext> allBuffers = new LinkedList<>();

    private final TieredStoreMemoryManager tieredStoreMemoryManager;

    private final PartitionFileWriter partitionFileWriter;

    @Nullable private final BufferCompressor bufferCompressor;

    @Nullable private OutputMetrics outputMetrics;

    private volatile boolean isClosed;

    private volatile boolean isReleased;

    private volatile CompletableFuture<Void> hasFlushCompleted =
            CompletableFuture.completedFuture(null);

    private final AtomicInteger currentSegmentId = new AtomicInteger(-1);

    public SubpartitionRemoteCacheManager(
            int targetChannel,
            int bufferSize,
            TieredStoreMemoryManager tieredStoreMemoryManager,
            CacheFlushManager cacheFlushManager,
            @Nullable BufferCompressor bufferCompressor,
            PartitionFileWriter partitionFileWriter) {
        this.targetChannel = targetChannel;
        this.bufferSize = bufferSize;
        this.tieredStoreMemoryManager = tieredStoreMemoryManager;
        this.bufferCompressor = bufferCompressor;
        cacheFlushManager.registerCacheSpillTrigger(this::flushCachedBuffers);
        this.partitionFileWriter = partitionFileWriter;
    }

    // ------------------------------------------------------------------------
    //  Called by DfsCacheDataManager
    // ------------------------------------------------------------------------

    public void append(ByteBuffer record, Buffer.DataType dataType, boolean isLastBufferInSegment)
            throws InterruptedException {
        if (dataType.isEvent()) {
            writeEvent(record, dataType, isLastBufferInSegment);
        } else {
            writeRecord(record, dataType, isLastBufferInSegment);
        }
    }

    public void setOutputMetrics(OutputMetrics outputMetrics) {
        this.outputMetrics = checkNotNull(outputMetrics);
    }

    public void startSegment(int segmentIndex) throws IOException {
        currentSegmentId.set(segmentIndex);
    }

    public void finishSegment(int segmentIndex) {
        checkState(currentSegmentId.get() == segmentIndex);
        flushCachedBuffers();
        partitionFileWriter.finishSegment(targetChannel, segmentIndex);
        checkState(allBuffers.isEmpty(), "Leaking finished buffers.");
    }

    /** Release all buffers. */
    public void release() {
        if (!isReleased) {
            for (BufferContext bufferContext : allBuffers) {
                Buffer buffer = bufferContext.getBuffer();
                if (!buffer.isRecycled()) {
                    buffer.recycleBuffer();
                }
            }
            allBuffers.clear();
            isReleased = true;
        }
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    private void writeEvent(
            ByteBuffer event, Buffer.DataType dataType, boolean isLastBufferInSegment) {
        checkArgument(dataType.isEvent());

        // each Event must take an exclusive buffer
        finishCurrentWritingBufferIfNotEmpty();

        // store Events in adhoc heap segments, for network memory efficiency
        MemorySegment data = MemorySegmentFactory.wrap(event.array());
        Buffer buffer =
                new NetworkBuffer(data, FreeingBufferRecycler.INSTANCE, dataType, data.size());

        BufferContext bufferContext = new BufferContext(buffer, finishedBufferIndex, targetChannel);
        addFinishedBuffer(bufferContext);
    }

    private void writeRecord(
            ByteBuffer record, Buffer.DataType dataType, boolean isLastBufferInSegment)
            throws InterruptedException {
        checkArgument(!dataType.isEvent());

        ensureCapacityForRecord(record);

        writeRecord(record, isLastBufferInSegment);
    }

    private void ensureCapacityForRecord(ByteBuffer record) {
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

    private void writeRecord(ByteBuffer record, boolean isLastBufferInSegment) {
        while (record.hasRemaining()) {
            BufferBuilder currentWritingBuffer =
                    checkNotNull(
                            unfinishedBuffers.peek(), "Expect enough capacity for the record.");
            currentWritingBuffer.append(record);
            if (currentWritingBuffer.isFull() && record.hasRemaining()) {
                finishCurrentWritingBuffer(false);
            } else if (currentWritingBuffer.isFull() && !record.hasRemaining()) {
                finishCurrentWritingBuffer(isLastBufferInSegment);
            } else if (!currentWritingBuffer.isFull() && !record.hasRemaining()) {
                if (isLastBufferInSegment) {
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
                        compressBuffersIfPossible(buffer), finishedBufferIndex, targetChannel);
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
            checkFlushCacheBuffers(tieredStoreMemoryManager, this::flushCachedBuffersWithCheck);
        }
    }

    private void recycleBuffer(MemorySegment buffer) {
        tieredStoreMemoryManager.recycleBuffer(buffer, TieredStoreMode.TieredType.IN_DFS);
    }

    void addFinishedBuffer(Buffer buffer) {
        BufferContext toAddBuffer = new BufferContext(buffer, finishedBufferIndex, targetChannel);
        addFinishedBuffer(toAddBuffer);
    }

    @SuppressWarnings("FieldAccessNotGuarded")
    // Note that: callWithLock ensure that code block guarded by resultPartitionReadLock and
    // subpartitionLock.
    private void addFinishedBuffer(BufferContext bufferContext) {
        finishedBufferIndex++;
        allBuffers.add(bufferContext);
        updateStatistics(bufferContext.getBuffer());
        if (allBuffers.size() >= NUM_BUFFERS_TO_FLUSH) {
            flushCachedBuffers();
        }
    }

    private void flushCachedBuffers() {
        List<BufferContext> bufferContexts = generateToSpillBuffersWithId();
        if (bufferContexts.size() > 0) {
            partitionFileWriter.spillAsync(targetChannel, currentSegmentId.get(), bufferContexts);
        }
    }

    private void flushCachedBuffersWithCheck() {
        hasFlushCompleted = new CompletableFuture<>();
        List<BufferContext> bufferContexts = generateToSpillBuffersWithId();
        if (bufferContexts.size() > 0) {
            CompletableFuture<Void> completableFuture =
                    partitionFileWriter.spillAsync(
                            targetChannel, currentSegmentId.get(), bufferContexts);
            completableFuture.thenRun(() -> hasFlushCompleted.complete(null));
        }
    }

    private List<BufferContext> generateToSpillBuffersWithId() {
        synchronized (allBuffers) {
            List<BufferContext> targetBuffers = new ArrayList<>(allBuffers);
            allBuffers.clear();
            return targetBuffers;
        }
    }

    private void updateStatistics(Buffer buffer) {
        checkNotNull(outputMetrics).getNumBuffersOut().inc();
        checkNotNull(outputMetrics).getNumBytesOut().inc(buffer.readableBytes());
    }

    void close() {
        isClosed = true;
        flushCachedBuffers();
        while (!unfinishedBuffers.isEmpty()) {
            unfinishedBuffers.poll().close();
        }
    }

    @VisibleForTesting
    public Path getBaseSubpartitionPath() {
        // return new Path(((RemoteCacheBufferSpiller)
        // cacheBufferSpiller).getBaseSubpartitionPath());
        return null;
    }
}
