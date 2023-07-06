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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.memory;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.api.EndOfSegmentEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyServiceProducer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageResourceRegistry;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierProducerAgent;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;
import java.util.Arrays;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.END_OF_SEGMENT;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageUtils.updateBufferRecyclerAndCompressBuffer;
import static org.apache.flink.util.Preconditions.checkArgument;

/** The DataManager of LOCAL file. */
public class MemoryTierProducerAgent implements TierProducerAgent, NettyServiceProducer {

    private final int numBuffersPerSegment;

    private final BufferCompressor bufferCompressor;

    private final TieredStorageMemoryManager storageMemoryManager;

    /** Record the byte number currently written to each subpartition. */
    private final int[] currentSubpartitionWriteBuffers;

    /**
     * Each element of the list is all views of the subpartition corresponding to its index, which
     * are stored in the form of a map that maps consumer id to its subpartition view.
     */
    private final boolean[] nettyServiceRegistered;

    private final MemoryTierSubpartitionProducerAgent[] subpartitionProducerAgents;

    public MemoryTierProducerAgent(
            TieredStoragePartitionId partitionId,
            int numSubpartitions,
            int bufferSize,
            int numBytesPerSegment,
            BufferCompressor bufferCompressor,
            TieredStorageMemoryManager storageMemoryManager,
            boolean isBroadcastOnly,
            TieredStorageNettyService nettyService,
            TieredStorageResourceRegistry resourceRegistry) {
        checkArgument(
                numBytesPerSegment >= bufferSize, "One segment contains at least one buffer.");
        checkArgument(
                !isBroadcastOnly,
                "Broadcast only partition is not allowed to use the memory tier.");

        this.numBuffersPerSegment = numBytesPerSegment / bufferSize;
        this.bufferCompressor = bufferCompressor;
        this.storageMemoryManager = storageMemoryManager;
        this.currentSubpartitionWriteBuffers = new int[numSubpartitions];
        this.nettyServiceRegistered = new boolean[numSubpartitions];
        this.subpartitionProducerAgents = new MemoryTierSubpartitionProducerAgent[numSubpartitions];

        Arrays.fill(currentSubpartitionWriteBuffers, 0);
        nettyService.registerProducer(partitionId, this);
        for (int subpartitionId = 0; subpartitionId < numSubpartitions; ++subpartitionId) {
            subpartitionProducerAgents[subpartitionId] =
                    new MemoryTierSubpartitionProducerAgent(subpartitionId, nettyService);
        }
        resourceRegistry.registerResource(partitionId, this::releaseResources);
    }

    @Override
    public boolean tryStartNewSegment(
            TieredStorageSubpartitionId subpartitionId,
            int segmentId,
            boolean forceUseCurrentTier) {
        boolean canStartNewSegment =
                nettyServiceRegistered[subpartitionId.getSubpartitionId()]
                        && (storageMemoryManager.getMaxNonReclaimableBuffers(this)
                                        - storageMemoryManager.numOwnerRequestedBuffer(this))
                                > numBuffersPerSegment;
        if (canStartNewSegment || forceUseCurrentTier) {
            subpartitionProducerAgents[subpartitionId.getSubpartitionId()].addSegmentBufferContext(
                    segmentId);
        }
        return canStartNewSegment || forceUseCurrentTier;
    }

    @Override
    public void release() {}

    @Override
    public boolean tryWrite(int subpartitionIndex, Buffer finishedBuffer) {
        if (currentSubpartitionWriteBuffers[subpartitionIndex] != 0
                && currentSubpartitionWriteBuffers[subpartitionIndex] + 1 > numBuffersPerSegment) {
            appendEndOfSegmentEvent(subpartitionIndex);
            currentSubpartitionWriteBuffers[subpartitionIndex] = 0;
            return false;
        }
        currentSubpartitionWriteBuffers[subpartitionIndex]++;
        addFinishedBuffer(
                updateBufferRecyclerAndCompressBuffer(
                        bufferCompressor,
                        finishedBuffer,
                        storageMemoryManager.getOwnerBufferRecycler(this)),
                subpartitionIndex);
        return true;
    }

    @Override
    public void connectionEstablished(
            TieredStorageSubpartitionId subpartitionId,
            NettyConnectionWriter nettyConnectionWriter) {
        this.subpartitionProducerAgents[subpartitionId.getSubpartitionId()].registerNettyService(
                nettyConnectionWriter);
        nettyServiceRegistered[subpartitionId.getSubpartitionId()] = true;
    }

    @Override
    public void connectionBroken(NettyConnectionId connectionId) {
        // noop
    }

    private void appendEndOfSegmentEvent(int subpartitionId) {
        try {
            MemorySegment memorySegment =
                    MemorySegmentFactory.wrap(
                            EventSerializer.toSerializedEvent(EndOfSegmentEvent.INSTANCE).array());
            addFinishedBuffer(
                    new NetworkBuffer(
                            memorySegment,
                            FreeingBufferRecycler.INSTANCE,
                            END_OF_SEGMENT,
                            memorySegment.size()),
                    subpartitionId);
        } catch (IOException e) {
            ExceptionUtils.rethrow(e, "Failed to append end of segment event,");
        }
    }

    private void addFinishedBuffer(Buffer finishedBuffer, int subpartitionId) {
        subpartitionProducerAgents[subpartitionId].addFinishedBuffer(finishedBuffer);
    }

    @Override
    public void close() {}

    // ------------------------------------
    //           Internal Method
    // ------------------------------------

    private void releaseResources() {
        Arrays.stream(subpartitionProducerAgents)
                .forEach(MemoryTierSubpartitionProducerAgent::release);
    }
}
