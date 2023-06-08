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

import org.apache.flink.runtime.io.network.api.EndOfSegmentEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
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

import static org.apache.flink.util.Preconditions.checkArgument;

/** The DataManager of LOCAL file. */
public class MemoryTierProducerAgent implements TierProducerAgent, NettyServiceProducer {

    private final int numBytesPerSegment;

    private final int numBuffersPerSegment;

    private final TieredStorageMemoryManager storageMemoryManager;

    private final boolean isBroadcastOnly;

    // Record the byte number currently written to each sub partition.
    private final int[] numSubpartitionEmitBytes;

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
            TieredStorageMemoryManager storageMemoryManager,
            boolean isBroadcastOnly,
            TieredStorageNettyService nettyService,
            TieredStorageResourceRegistry resourceRegistry) {
        checkArgument(
                numBytesPerSegment >= bufferSize, "One segment contains at least one buffer.");

        this.numBytesPerSegment = numBytesPerSegment;
        this.numBuffersPerSegment = numBytesPerSegment / bufferSize;
        this.isBroadcastOnly = isBroadcastOnly;
        this.storageMemoryManager = storageMemoryManager;
        this.numSubpartitionEmitBytes = new int[numSubpartitions];
        this.nettyServiceRegistered = new boolean[numSubpartitions];
        this.subpartitionProducerAgents = new MemoryTierSubpartitionProducerAgent[numSubpartitions];

        Arrays.fill(numSubpartitionEmitBytes, 0);
        nettyService.registerProducer(partitionId, this);
        for (int subpartitionId = 0; subpartitionId < numSubpartitions; ++subpartitionId) {
            subpartitionProducerAgents[subpartitionId] =
                    new MemoryTierSubpartitionProducerAgent(subpartitionId, nettyService);
        }
        resourceRegistry.registerResource(partitionId, this::releaseResources);
    }

    private void register(
            TieredStorageSubpartitionId subpartitionId,
            NettyConnectionWriter nettyConnectionWriter) {
        if (isBroadcastOnly) {
            throw new RuntimeException("Illegal to register on broadcast only result partition.");
        }
        this.subpartitionProducerAgents[subpartitionId.getSubpartitionId()].registerNettyService(
                nettyConnectionWriter);
        nettyServiceRegistered[subpartitionId.getSubpartitionId()] = true;
    }

    @Override
    public boolean tryStartNewSegment(
            TieredStorageSubpartitionId subpartitionId,
            int segmentId,
            boolean forceUseCurrentTier) {
        if (isBroadcastOnly && !forceUseCurrentTier) {
            return false;
        }
        boolean canStartNewSegment =
                isSubpartitionRegistered(subpartitionId)
                        && (storageMemoryManager.getMaxNonReclaimableBuffers(this)
                                        - storageMemoryManager.numOwnerRequestedBuffer(this))
                                > numBuffersPerSegment;
        if (canStartNewSegment || forceUseCurrentTier) {
            getSubpartitionMemoryDataManager(subpartitionId.getSubpartitionId())
                    .addSegmentBufferContext(segmentId);
        }
        return canStartNewSegment || forceUseCurrentTier;
    }

    @Override
    public void release() {}

    @Override
    public boolean tryWrite(int consumerId, Buffer finishedBuffer) {
        if (numSubpartitionEmitBytes[consumerId] != 0
                && numSubpartitionEmitBytes[consumerId] + finishedBuffer.readableBytes()
                        > numBytesPerSegment) {
            appendEndOfSegmentEvent(consumerId);
            numSubpartitionEmitBytes[consumerId] = 0;
            return false;
        }
        numSubpartitionEmitBytes[consumerId] += finishedBuffer.readableBytes();
        append(finishedBuffer, consumerId);
        return true;
    }

    @Override
    public void connectionEstablished(
            TieredStorageSubpartitionId subpartitionId,
            NettyConnectionWriter nettyConnectionWriter) {
        if (isBroadcastOnly) {
            throw new RuntimeException("Illegal to register on broadcast only result partition.");
        }
        this.subpartitionProducerAgents[subpartitionId.getSubpartitionId()].registerNettyService(
                nettyConnectionWriter);
        nettyServiceRegistered[subpartitionId.getSubpartitionId()] = true;
    }

    @Override
    public void connectionBroken(NettyConnectionId connectionId) {
        // noop
    }

    private void appendEndOfSegmentEvent(int targetChannel) {
        try {
            getSubpartitionMemoryDataManager(targetChannel)
                    .appendSegmentEvent(
                            EventSerializer.toSerializedEvent(EndOfSegmentEvent.INSTANCE));
        } catch (IOException e) {
            ExceptionUtils.rethrow(e, "Failed to append end of segment event,");
        }
    }

    private void append(Buffer finishedBuffer, int targetChannel) {
        getSubpartitionMemoryDataManager(targetChannel).addFinishedBuffer(finishedBuffer);
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

    private boolean isSubpartitionRegistered(TieredStorageSubpartitionId subpartitionId) {
        return nettyServiceRegistered[subpartitionId.getSubpartitionId()];
    }

    private MemoryTierSubpartitionProducerAgent getSubpartitionMemoryDataManager(
            int targetChannel) {
        return subpartitionProducerAgents[targetChannel];
    }
}
