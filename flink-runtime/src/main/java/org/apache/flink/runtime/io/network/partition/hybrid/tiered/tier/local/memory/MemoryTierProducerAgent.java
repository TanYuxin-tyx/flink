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

import org.apache.flink.runtime.io.network.api.EndOfSegmentEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStoragePartitionIdAndSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierProducerAgent;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;
import java.util.Arrays;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.END_OF_SEGMENT;

/** The DataManager of LOCAL file. */
public class MemoryTierProducerAgent implements TierProducerAgent {

    private final int numSubpartitions;

    private final int numBytesPerSegment;

    private final int numBuffersPerSegment;

    private final TieredStorageMemoryManager storageMemoryManager;

    private final boolean isBroadcastOnly;

    private volatile boolean isReleased;

    // Record the byte number currently written to each sub partition.
    private final int[] numSubpartitionEmitBytes;

    /**
     * Each element of the list is all views of the subpartition corresponding to its index, which
     * are stored in the form of a map that maps consumer id to its subpartition view.
     */
    private final boolean[] nettyServiceRegistered;

    private final MemoryTierSubpartitionProducerAgent[] subpartitionProducerAgents;

    public MemoryTierProducerAgent(
            int numSubpartitions,
            int numBytesPerSegment,
            TieredStorageMemoryManager storageMemoryManager,
            boolean isBroadcastOnly,
            int bufferSize,
            TieredStorageNettyService nettyService) {
        this.numSubpartitions = numSubpartitions;
        this.numBytesPerSegment = numBytesPerSegment;
        this.numBuffersPerSegment = numBytesPerSegment / 32 / 1024;
        this.isBroadcastOnly = isBroadcastOnly;
        this.storageMemoryManager = storageMemoryManager;

        this.numSubpartitionEmitBytes = new int[numSubpartitions];
        Arrays.fill(numSubpartitionEmitBytes, 0);
        this.nettyServiceRegistered = new boolean[numSubpartitions];
        this.subpartitionProducerAgents = new MemoryTierSubpartitionProducerAgent[numSubpartitions];
        for (int subpartitionId = 0; subpartitionId < numSubpartitions; ++subpartitionId) {
            subpartitionProducerAgents[subpartitionId] =
                    new MemoryTierSubpartitionProducerAgent(
                            subpartitionId, bufferSize, nettyService);
        }
    }

    @Override
    public void registerNettyService(
            int subpartitionId, TieredStoragePartitionIdAndSubpartitionId nettyServiceWriterId) {
        if (isBroadcastOnly) {
            throw new RuntimeException("Illegal to register on broadcast only result partition.");
        }
        this.subpartitionProducerAgents[subpartitionId].registerNettyService(nettyServiceWriterId);
        nettyServiceRegistered[subpartitionId] = true;
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
    public void release() {
        for (int i = 0; i < numSubpartitions; i++) {
            getSubpartitionMemoryDataManagers()[i].release();
        }

        if (!isReleased) {
            isReleased = true;
        }
    }

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

    private void appendEndOfSegmentEvent(int targetChannel) {
        try {
            getSubpartitionMemoryDataManager(targetChannel)
                    .appendSegmentEvent(
                            EventSerializer.toSerializedEvent(EndOfSegmentEvent.INSTANCE),
                            END_OF_SEGMENT);
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

    private boolean isSubpartitionRegistered(TieredStorageSubpartitionId subpartitionId) {
        return nettyServiceRegistered[subpartitionId.getSubpartitionId()];
    }

    private MemoryTierSubpartitionProducerAgent getSubpartitionMemoryDataManager(
            int targetChannel) {
        return subpartitionProducerAgents[targetChannel];
    }

    public MemoryTierSubpartitionProducerAgent[] getSubpartitionMemoryDataManagers() {
        return subpartitionProducerAgents;
    }
}
