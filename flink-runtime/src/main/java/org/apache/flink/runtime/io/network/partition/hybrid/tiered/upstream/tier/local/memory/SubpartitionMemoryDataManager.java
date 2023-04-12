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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.tier.local.memory;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.Buffer.DataType;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.BufferContext;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.NettyBasedTierConsumerViewId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.OutputMetrics;
import org.apache.flink.util.function.SupplierWithException;
import org.apache.flink.util.function.ThrowingRunnable;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * This class is responsible for managing the data in a single subpartition. One {@link
 * MemoryTierStorageWriter} will hold multiple {@link MemoryTierConsumer}.
 */
public class SubpartitionMemoryDataManager {

    private final int targetChannel;

    private final int bufferSize;

    private final MemoryDataWriterOperation memoryDataWriterOperation;

    // Not guarded by lock because it is expected only accessed from task's main thread.
    private final Queue<BufferBuilder> unfinishedBuffers = new LinkedList<>();

    // Not guarded by lock because it is expected only accessed from task's main thread.
    private int finishedBufferIndex;

    @GuardedBy("subpartitionLock")
    private final Deque<BufferContext> allBuffers = new LinkedList<>();

    /** DO NOT USE DIRECTLY. Use {@link #runWithLock} or {@link #callWithLock} instead. */
    private final ReentrantReadWriteLock subpartitionLock = new ReentrantReadWriteLock();

    @GuardedBy("subpartitionLock")
    private final Map<NettyBasedTierConsumerViewId, MemoryTierConsumer> consumerMap;

    @Nullable private final BufferCompressor bufferCompressor;

    @Nullable private OutputMetrics outputMetrics;

    public SubpartitionMemoryDataManager(
            int targetChannel,
            int bufferSize,
            @Nullable BufferCompressor bufferCompressor,
            MemoryDataWriterOperation memoryDataWriterOperation) {
        this.targetChannel = targetChannel;
        this.bufferSize = bufferSize;
        this.memoryDataWriterOperation = memoryDataWriterOperation;
        this.bufferCompressor = bufferCompressor;
        this.consumerMap = new HashMap<>();
    }

    // ------------------------------------------------------------------------
    //  Called by MemoryDataManager
    // ------------------------------------------------------------------------

    /**
     * Append record to {@link MemoryTierConsumer}.
     *
     * @param record to be managed by this class.
     * @param dataType the type of this record. In other words, is it data or event.
     */
    public void appendSegmentEvent(ByteBuffer record, DataType dataType) {
        writeEvent(record, dataType);
    }

    public void release() {
        for (BufferContext bufferContext : allBuffers) {
            if (!bufferContext.getBuffer().isRecycled()) {
                bufferContext.getBuffer().recycleBuffer();
            }
        }
        allBuffers.clear();
    }

    @SuppressWarnings("FieldAccessNotGuarded")
    public MemoryTierConsumer registerNewConsumer(
            NettyBasedTierConsumerViewId nettyBasedTierConsumerViewId) {
        return callWithLock(
                () -> {
                    checkState(!consumerMap.containsKey(nettyBasedTierConsumerViewId));
                    MemoryTierConsumer newConsumer =
                            new MemoryTierConsumer(
                                    subpartitionLock.readLock(),
                                    targetChannel,
                                    nettyBasedTierConsumerViewId,
                                    memoryDataWriterOperation);
                    newConsumer.addInitialBuffers(allBuffers);
                    consumerMap.put(nettyBasedTierConsumerViewId, newConsumer);
                    return newConsumer;
                });
    }

    @SuppressWarnings("FieldAccessNotGuarded")
    public void releaseConsumer(NettyBasedTierConsumerViewId nettyBasedTierConsumerViewId) {
        runWithLock(() -> checkNotNull(consumerMap.remove(nettyBasedTierConsumerViewId)));
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    private void writeEvent(ByteBuffer event, DataType dataType) {
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

    private void finishCurrentWritingBufferIfNotEmpty() {
        BufferBuilder currentWritingBuffer = unfinishedBuffers.peek();
        if (currentWritingBuffer == null || currentWritingBuffer.getWritableBytes() == bufferSize) {
            return;
        }

        finishCurrentWritingBuffer();
    }

    private void finishCurrentWritingBuffer() {
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

    void addFinishedBuffer(Buffer buffer) {
        BufferContext toAddBuffer = new BufferContext(buffer, finishedBufferIndex, targetChannel);
        addFinishedBuffer(toAddBuffer);
    }

    @SuppressWarnings("FieldAccessNotGuarded")
    // Note that: callWithLock ensure that code block guarded by resultPartitionReadLock and
    // subpartitionLock.
    private void addFinishedBuffer(BufferContext bufferContext) {
        List<NettyBasedTierConsumerViewId> needNotify = new ArrayList<>(consumerMap.size());
        runWithLock(
                () -> {
                    finishedBufferIndex++;
                    if (consumerMap.size() == 0) {
                        allBuffers.add(bufferContext);
                    }
                    for (Map.Entry<NettyBasedTierConsumerViewId, MemoryTierConsumer> consumerEntry :
                            consumerMap.entrySet()) {
                        if (consumerEntry.getValue().addBuffer(bufferContext)) {
                            needNotify.add(consumerEntry.getKey());
                        }
                    }
                });
        memoryDataWriterOperation.onDataAvailable(targetChannel, needNotify);
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
