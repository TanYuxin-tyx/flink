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

package org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.memory;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.Buffer.DataType;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.BufferContext;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreMemoryManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierReaderViewId;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.OutputMetrics;
import org.apache.flink.util.function.SupplierWithException;
import org.apache.flink.util.function.ThrowingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.flink.runtime.io.network.partition.tieredstore.upstream.TieredStoreMode.TieredType.IN_MEM;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * This class is responsible for managing the data in a single subpartition. One {@link
 * MemoryWriter} will hold multiple {@link SubpartitionConsumerMemoryReader}.
 */
public class SubpartitionMemoryDataManager {

    private static final Logger LOG = LoggerFactory.getLogger(SubpartitionMemoryDataManager.class);

    private final int targetChannel;

    private final int bufferSize;

    private final int numTotalConsumers;

    private final MemoryDataWriterOperation memoryDataWriterOperation;

    // Not guarded by lock because it is expected only accessed from task's main thread.
    private final Queue<BufferBuilder> unfinishedBuffers = new LinkedList<>();

    // Not guarded by lock because it is expected only accessed from task's main thread.
    private int finishedBufferIndex;

    @GuardedBy("subpartitionLock")
    private final Deque<BufferContext> allBuffers = new LinkedList<>();

    /** DO NOT USE DIRECTLY. Use {@link #runWithLock} or {@link #callWithLock} instead. */
    private final ReentrantReadWriteLock subpartitionLock = new ReentrantReadWriteLock();

    private final TieredStoreMemoryManager tieredStoreMemoryManager;

    @GuardedBy("subpartitionLock")
    private final Map<TierReaderViewId, SubpartitionConsumerMemoryReader> consumerMap;

    @Nullable private final BufferCompressor bufferCompressor;

    @Nullable private OutputMetrics outputMetrics;

    public SubpartitionMemoryDataManager(
            int targetChannel,
            int bufferSize,
            int numTotalConsumers,
            @Nullable BufferCompressor bufferCompressor,
            MemoryDataWriterOperation memoryDataWriterOperation,
            TieredStoreMemoryManager tieredStoreMemoryManager) {
        this.targetChannel = targetChannel;
        this.bufferSize = bufferSize;
        this.numTotalConsumers = numTotalConsumers;
        this.memoryDataWriterOperation = memoryDataWriterOperation;
        this.bufferCompressor = bufferCompressor;
        this.tieredStoreMemoryManager = tieredStoreMemoryManager;
        this.consumerMap = new HashMap<>();
    }

    // ------------------------------------------------------------------------
    //  Called by MemoryDataManager
    // ------------------------------------------------------------------------

    /**
     * Append record to {@link SubpartitionConsumerMemoryReader}.
     *
     * @param record to be managed by this class.
     * @param dataType the type of this record. In other words, is it data or event.
     * @param isLastRecordInSegment whether this record is the last record in a segment.
     */
    public void append(
            ByteBuffer record,
            DataType dataType,
            boolean isBroadcast,
            boolean isLastRecordInSegment)
            throws InterruptedException {
        if (dataType.isEvent()) {
            writeEvent(record, dataType, isBroadcast, isLastRecordInSegment);
        } else {
            writeRecord(record, dataType, isBroadcast, isLastRecordInSegment);
        }
    }

    public void setOutputMetrics(OutputMetrics outputMetrics) {
        this.outputMetrics = checkNotNull(outputMetrics);
    }

    public void release() {
        for(BufferContext bufferContext : allBuffers){
            bufferContext.getBuffer().recycleBuffer();
        }
        allBuffers.clear();
    }

    @SuppressWarnings("FieldAccessNotGuarded")
    public SubpartitionConsumerMemoryReader registerNewConsumer(TierReaderViewId tierReaderViewId) {
        return callWithLock(
                () -> {
                    checkState(!consumerMap.containsKey(tierReaderViewId));
                    SubpartitionConsumerMemoryReader newConsumer =
                            new SubpartitionConsumerMemoryReader(
                                    subpartitionLock.readLock(),
                                    targetChannel,
                                    tierReaderViewId,
                                    memoryDataWriterOperation);
                    newConsumer.addInitialBuffers(allBuffers);
                    consumerMap.put(tierReaderViewId, newConsumer);
                    return newConsumer;
                });
    }

    @SuppressWarnings("FieldAccessNotGuarded")
    public void releaseConsumer(TierReaderViewId tierReaderViewId) {
        runWithLock(() -> checkNotNull(consumerMap.remove(tierReaderViewId)));
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    private void writeEvent(
            ByteBuffer event,
            DataType dataType,
            boolean isBroadcast,
            boolean isLastRecordInSegment) {
        checkArgument(dataType.isEvent());

        // each Event must take an exclusive buffer
        finishCurrentWritingBufferIfNotEmpty(isBroadcast);

        // store Events in adhoc heap segments, for network memory efficiency
        MemorySegment data = MemorySegmentFactory.wrap(event.array());
        Buffer buffer =
                new NetworkBuffer(data, FreeingBufferRecycler.INSTANCE, dataType, data.size());

        BufferContext bufferContext =
                new BufferContext(
                        buffer, finishedBufferIndex, targetChannel, isLastRecordInSegment);
        addFinishedBuffer(isBroadcast, bufferContext);
    }

    private void writeRecord(
            ByteBuffer record,
            DataType dataType,
            boolean isBroadcast,
            boolean isLastRecordInSegment)
            throws InterruptedException {
        checkArgument(!dataType.isEvent());

        ensureCapacityForRecord(record);

        writeRecord(record, isBroadcast, isLastRecordInSegment);
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
            MemorySegment memorySegment =
                    memoryDataWriterOperation.requestBufferFromPool(targetChannel);
            unfinishedBuffers.add(new BufferBuilder(memorySegment, this::recycle));
            availableBytes += bufferSize;
        }
    }

    private void writeRecord(
            ByteBuffer record, boolean isBroadcast, boolean isLastRecordInSegment) {
        while (record.hasRemaining()) {
            BufferBuilder currentWritingBuffer =
                    checkNotNull(
                            unfinishedBuffers.peek(), "Expect enough capacity for the record.");
            currentWritingBuffer.append(record);
            if (currentWritingBuffer.isFull() && record.hasRemaining()) {
                finishCurrentWritingBuffer(isBroadcast, false);
            } else if (currentWritingBuffer.isFull() && !record.hasRemaining()) {
                finishCurrentWritingBuffer(isBroadcast, isLastRecordInSegment);
            } else if (!currentWritingBuffer.isFull() && !record.hasRemaining()) {
                if (isLastRecordInSegment) {
                    finishCurrentWritingBuffer(isBroadcast, true);
                }
            }
        }
    }

    private void finishCurrentWritingBufferIfNotEmpty(boolean isBroadcast) {
        BufferBuilder currentWritingBuffer = unfinishedBuffers.peek();
        if (currentWritingBuffer == null || currentWritingBuffer.getWritableBytes() == bufferSize) {
            return;
        }

        finishCurrentWritingBuffer(isBroadcast, false);
    }

    private void finishCurrentWritingBuffer(boolean isBroadcast, boolean isLastBufferInSegment) {
        BufferBuilder currentWritingBuffer = unfinishedBuffers.poll();
        if (currentWritingBuffer == null) {
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
        addFinishedBuffer(isBroadcast, bufferContext);
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

    @SuppressWarnings("FieldAccessNotGuarded")
    // Note that: callWithLock ensure that code block guarded by resultPartitionReadLock and
    // subpartitionLock.
    private void addFinishedBuffer(boolean isBroadcast, BufferContext bufferContext) {
        List<TierReaderViewId> needNotify = new ArrayList<>(consumerMap.size());
        runWithLock(
                () -> {
                    finishedBufferIndex++;
                    if (consumerMap.size() == 0) {
                        allBuffers.add(bufferContext);
                    }
                    retainBufferIfNeeded(isBroadcast, bufferContext);
                    for (Map.Entry<TierReaderViewId, SubpartitionConsumerMemoryReader>
                            consumerEntry : consumerMap.entrySet()) {
                        if (consumerEntry.getValue().addBuffer(bufferContext)) {
                            needNotify.add(consumerEntry.getKey());
                        }
                    }
                    updateStatistics(bufferContext.getBuffer());
                });
        memoryDataWriterOperation.onDataAvailable(targetChannel, needNotify);
    }

    private void retainBufferIfNeeded(boolean isBroadcast, BufferContext bufferContext) {
        if (isBroadcast) {
            for (int i = 0; i < numTotalConsumers - 1; i++) {
                bufferContext.getBuffer().retainBuffer();
            }
        }
    }

    private void recycle(MemorySegment memorySegment) {
        tieredStoreMemoryManager.recycleBuffer(memorySegment, IN_MEM);
    }

    private void updateStatistics(Buffer buffer) {
        checkNotNull(outputMetrics).getNumBuffersOut().inc();
        checkNotNull(outputMetrics).getNumBytesOut().inc(buffer.readableBytes());
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
}
