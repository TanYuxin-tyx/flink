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
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.BufferContext;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.netty.NettyBufferQueue;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.netty.NettyBufferQueueImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.netty.NettyServiceViewId;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** TODO. */
public class SubpartitionMemoryDataManager {

    private final int targetChannel;

    private final int bufferSize;

    private final MemoryTierProducerAgentOperation memoryTierProducerAgentOperation;

    // Not guarded by lock because it is expected only accessed from task's main thread.
    private final Queue<BufferBuilder> unfinishedBuffers = new LinkedList<>();

    // Not guarded by lock because it is expected only accessed from task's main thread.
    private int finishedBufferIndex;

    @GuardedBy("subpartitionLock")
    private final LinkedBlockingDeque<BufferContext> allBuffers = new LinkedBlockingDeque<>();

    private final Map<NettyServiceViewId, NettyBufferQueue> consumerMap;

    @Nullable private final BufferCompressor bufferCompressor;

    public SubpartitionMemoryDataManager(
            int targetChannel,
            int bufferSize,
            @Nullable BufferCompressor bufferCompressor,
            MemoryTierProducerAgentOperation memoryTierProducerAgentOperation) {
        this.targetChannel = targetChannel;
        this.bufferSize = bufferSize;
        this.memoryTierProducerAgentOperation = memoryTierProducerAgentOperation;
        this.bufferCompressor = bufferCompressor;
        this.consumerMap = new HashMap<>();
    }

    // ------------------------------------------------------------------------
    //  Called by MemoryDataManager
    // ------------------------------------------------------------------------

    public void appendSegmentEvent(ByteBuffer record, DataType dataType) {
        writeEvent(record, dataType);
    }

    public void release() {
        for (BufferContext bufferContext : allBuffers) {
            if (!checkNotNull(bufferContext.getBuffer()).isRecycled()) {
                bufferContext.getBuffer().recycleBuffer();
            }
        }
        allBuffers.clear();
    }

    public NettyBufferQueue registerNewConsumer(
            NettyServiceViewId nettyServiceViewId) {
        checkState(!consumerMap.containsKey(nettyServiceViewId));
        NettyBufferQueueImpl nettyServiceProviderImpl =
                new NettyBufferQueueImpl(
                        allBuffers,
                        () ->
                                memoryTierProducerAgentOperation.onConsumerReleased(
                                        targetChannel, nettyServiceViewId));
        consumerMap.put(nettyServiceViewId, nettyServiceProviderImpl);
        return nettyServiceProviderImpl;
    }

    @SuppressWarnings("FieldAccessNotGuarded")
    public void releaseConsumer(NettyServiceViewId nettyServiceViewId) {
        checkNotNull(consumerMap.remove(nettyServiceViewId));
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

    private void addFinishedBuffer(BufferContext bufferContext) {
        List<NettyServiceViewId> needNotify = new ArrayList<>(consumerMap.size());
        finishedBufferIndex++;
        allBuffers.add(bufferContext);
        for (Map.Entry<NettyServiceViewId, NettyBufferQueue> consumerEntry :
                consumerMap.entrySet()) {
            if (allBuffers.size() <= 1) {
                needNotify.add(consumerEntry.getKey());
            }
        }
        memoryTierProducerAgentOperation.onDataAvailable(targetChannel, needNotify);
    }
}
