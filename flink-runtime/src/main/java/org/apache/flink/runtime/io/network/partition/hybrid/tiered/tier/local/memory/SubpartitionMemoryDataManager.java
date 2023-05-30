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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.local.memory;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.Buffer.DataType;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.CreditBasedShuffleViewId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.netty2.NettyServiceWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.netty2.TieredStorageNettyService2;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.netty2.impl.TieredStorageNettyServiceImpl2;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.BufferContext;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** TODO. */
public class SubpartitionMemoryDataManager {

    private final int subpartitionId;

    private final int bufferSize;

    private final MemoryTierProducerAgentOperation memoryTierProducerAgentOperation;

    // Not guarded by lock because it is expected only accessed from task's main thread.
    private final Queue<BufferBuilder> unfinishedBuffers = new LinkedList<>();

    // Not guarded by lock because it is expected only accessed from task's main thread.
    private int finishedBufferIndex;

    private final Set<CreditBasedShuffleViewId> consumerSet;

    @Nullable private final BufferCompressor bufferCompressor;

    private final TieredStorageNettyService2 nettyService;

    private final NettyServiceWriter nettyServiceWriter;

    private ResultPartitionID partitionId;

    public SubpartitionMemoryDataManager(
            ResultPartitionID partitionId,
            int subpartitionId,
            int bufferSize,
            @Nullable BufferCompressor bufferCompressor,
            MemoryTierProducerAgentOperation memoryTierProducerAgentOperation,
            TieredStorageNettyService2 nettyService) {
        this.partitionId = partitionId;
        this.subpartitionId = subpartitionId;
        this.bufferSize = bufferSize;
        this.memoryTierProducerAgentOperation = memoryTierProducerAgentOperation;
        this.bufferCompressor = bufferCompressor;
        this.consumerSet = Collections.synchronizedSet(new HashSet<>());
        this.nettyService = nettyService;
        this.nettyServiceWriter =
                nettyService.registerProducer(partitionId, subpartitionId, () -> {});
    }

    // ------------------------------------------------------------------------
    //  Called by MemoryDataManager
    // ------------------------------------------------------------------------

    public void appendSegmentEvent(ByteBuffer record, DataType dataType) {
        writeEvent(record, dataType);
    }

    public void release() {
        nettyServiceWriter.clear();
    }

    public void registerNettyService(BufferAvailabilityListener availabilityListener) {
        // nothing to do.
    }

    @SuppressWarnings("FieldAccessNotGuarded")
    public void releaseConsumer(CreditBasedShuffleViewId creditBasedShuffleViewId) {
        checkNotNull(consumerSet.remove(creditBasedShuffleViewId));
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

        BufferContext bufferContext =
                new BufferContext(buffer, finishedBufferIndex, subpartitionId);
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
                        compressBuffersIfPossible(buffer), finishedBufferIndex, subpartitionId);
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
        BufferContext toAddBuffer = new BufferContext(buffer, finishedBufferIndex, subpartitionId);
        addFinishedBuffer(toAddBuffer);
    }

    private void addFinishedBuffer(BufferContext bufferContext) {
        finishedBufferIndex++;
        nettyServiceWriter.writeBuffer(bufferContext);
        if (nettyServiceWriter.size() <= 1) {
            ((TieredStorageNettyServiceImpl2) nettyService)
                    .notifyResultSubpartitionViewSendBuffer(partitionId, subpartitionId);
        }
    }
}
