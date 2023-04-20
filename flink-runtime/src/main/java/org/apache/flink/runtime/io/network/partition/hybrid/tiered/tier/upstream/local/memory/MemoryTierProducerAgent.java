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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.local.memory;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.api.EndOfSegmentEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TierType;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.common.SubpartitionSegmentIndexTracker;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.common.SubpartitionSegmentIndexTrackerImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.common.TierProducerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.common.TieredStoreMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.service.NettyBasedTierConsumer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.service.NettyBasedTierConsumerView;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.service.NettyBasedTierConsumerViewId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.service.NettyBasedTierConsumerViewImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.service.NettyServiceViewProvider;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.ADD_SEGMENT_ID_EVENT;
import static org.apache.flink.util.Preconditions.checkState;

/** The DataManager of LOCAL file. */
public class MemoryTierProducerAgent
        implements TierProducerAgent, NettyServiceViewProvider, MemoryTierProducerAgentOperation {

    public static final int BROADCAST_CHANNEL = 0;

    private final int numSubpartitions;

    private final TieredStoreMemoryManager tieredStoreMemoryManager;

    private final boolean isBroadcastOnly;

    /** Record the last assigned consumerId for each subpartition. */
    private final NettyBasedTierConsumerViewId[] lastNettyBasedTierConsumerViewIds;

    public static final int MEMORY_TIER_SEGMENT_BYTES = 10 * 32 * 1024;

    private final int bufferNumberInSegment = MEMORY_TIER_SEGMENT_BYTES / 32 / 1024;

    private volatile boolean isReleased;

    // Record the byte number currently written to each sub partition.
    private final int[] numSubpartitionEmitBytes;

    /**
     * Each element of the list is all views of the subpartition corresponding to its index, which
     * are stored in the form of a map that maps consumer id to its subpartition view.
     */
    private final List<Map<NettyBasedTierConsumerViewId, NettyBasedTierConsumerView>>
            subpartitionViewOperationsMap;

    private final SubpartitionMemoryDataManager[] subpartitionMemoryDataManagers;

    private final SubpartitionSegmentIndexTracker subpartitionSegmentIndexTracker;

    public MemoryTierProducerAgent(
            int numSubpartitions,
            TieredStoreMemoryManager tieredStoreMemoryManager,
            boolean isBroadcastOnly,
            BufferCompressor bufferCompressor,
            int bufferSize) {
        this.numSubpartitions = numSubpartitions;
        this.isBroadcastOnly = isBroadcastOnly;
        this.tieredStoreMemoryManager = tieredStoreMemoryManager;
        this.lastNettyBasedTierConsumerViewIds = new NettyBasedTierConsumerViewId[numSubpartitions];

        this.numSubpartitionEmitBytes = new int[numSubpartitions];
        Arrays.fill(numSubpartitionEmitBytes, 0);
        this.subpartitionViewOperationsMap = new ArrayList<>(numSubpartitions);
        this.subpartitionMemoryDataManagers = new SubpartitionMemoryDataManager[numSubpartitions];
        this.subpartitionSegmentIndexTracker =
                new SubpartitionSegmentIndexTrackerImpl(numSubpartitions, isBroadcastOnly);
        for (int subpartitionId = 0; subpartitionId < numSubpartitions; ++subpartitionId) {
            subpartitionMemoryDataManagers[subpartitionId] =
                    new SubpartitionMemoryDataManager(
                            subpartitionId, bufferSize, bufferCompressor, this);
            subpartitionViewOperationsMap.add(new ConcurrentHashMap<>());
        }
    }

    @Override
    public NettyBasedTierConsumerView createNettyBasedTierConsumerView(
            int subpartitionId, BufferAvailabilityListener availabilityListener) {
        // if broadcastOptimize is enabled, map every subpartitionId to the special broadcast
        // channel.
        subpartitionId = isBroadcastOnly ? BROADCAST_CHANNEL : subpartitionId;

        NettyBasedTierConsumerViewImpl memoryReaderView =
                new NettyBasedTierConsumerViewImpl(availabilityListener);
        NettyBasedTierConsumerViewId lastNettyBasedTierConsumerViewId =
                lastNettyBasedTierConsumerViewIds[subpartitionId];
        checkMultipleConsumerIsAllowed(lastNettyBasedTierConsumerViewId);
        // assign a unique id for each consumer, now it is guaranteed by the value that is one
        // higher than the last consumerId's id field.
        NettyBasedTierConsumerViewId nettyBasedTierConsumerViewId =
                NettyBasedTierConsumerViewId.newId(lastNettyBasedTierConsumerViewId);
        lastNettyBasedTierConsumerViewIds[subpartitionId] = nettyBasedTierConsumerViewId;

        NettyBasedTierConsumer memoryConsumer =
                registerNewConsumer(subpartitionId, nettyBasedTierConsumerViewId, memoryReaderView);

        memoryReaderView.setConsumer(memoryConsumer);
        return memoryReaderView;
    }

    @Override
    public boolean canStoreNextSegment(int consumerId) {
        return tieredStoreMemoryManager.numAvailableBuffers(TierType.IN_MEM) > bufferNumberInSegment
                && isConsumerRegistered(consumerId);
    }

    @Override
    public boolean hasCurrentSegment(int subpartitionId, int segmentIndex) {
        return getSegmentIndexTracker().hasCurrentSegment(subpartitionId, segmentIndex);
    }

    @Override
    public void release() {
        for (int i = 0; i < numSubpartitions; i++) {
            getSubpartitionMemoryDataManagers()[i].release();
        }

        // release is called when release by scheduler, later than close.
        // mainly work :
        // 1. release read scheduler.
        // 2. delete shuffle file.
        // 3. release all data in memory.

        if (!isReleased) {
            getSegmentIndexTracker().release();
            isReleased = true;
        }
    }

    @Override
    public TierType getTierType() {
        return TierType.IN_MEM;
    }

    private static void checkMultipleConsumerIsAllowed(
            NettyBasedTierConsumerViewId lastNettyBasedTierConsumerViewId) {
        checkState(
                lastNettyBasedTierConsumerViewId == null,
                "Memory Tier does not support multiple consumers");
    }

    @Override
    public void startSegment(int consumerId, int segmentId) {
        subpartitionSegmentIndexTracker.addSubpartitionSegmentIndex(consumerId, segmentId);
    }

    @Override
    public boolean write(int consumerId, Buffer finishedBuffer) {
        boolean isLastBufferInSegment = false;
        numSubpartitionEmitBytes[consumerId] += finishedBuffer.readableBytes();
        if (numSubpartitionEmitBytes[consumerId]
                >= MemoryTierProducerAgent.MEMORY_TIER_SEGMENT_BYTES) {
            isLastBufferInSegment = true;
            numSubpartitionEmitBytes[consumerId] = 0;
        }
        if (isLastBufferInSegment) {
            append(finishedBuffer, consumerId);
            // Send the EndOfSegmentEvent
            appendEndOfSegmentEvent(consumerId);
        } else {
            append(finishedBuffer, consumerId);
        }
        return isLastBufferInSegment;
    }

    private void appendEndOfSegmentEvent(int targetChannel) {
        try {
            getSubpartitionMemoryDataManager(targetChannel)
                    .appendSegmentEvent(
                            EventSerializer.toSerializedEvent(EndOfSegmentEvent.INSTANCE),
                            ADD_SEGMENT_ID_EVENT);
        } catch (IOException e) {
            ExceptionUtils.rethrow(e, "Failed to append end of segment event,");
        }
    }

    private void append(Buffer finishedBuffer, int targetChannel) {
        getSubpartitionMemoryDataManager(targetChannel).addFinishedBuffer(finishedBuffer);
    }

    public NettyBasedTierConsumer registerNewConsumer(
            int subpartitionId,
            NettyBasedTierConsumerViewId nettyBasedTierConsumerViewId,
            NettyBasedTierConsumerView viewOperations) {
        NettyBasedTierConsumerView oldView =
                subpartitionViewOperationsMap
                        .get(subpartitionId)
                        .put(nettyBasedTierConsumerViewId, viewOperations);
        Preconditions.checkState(
                oldView == null, "Each subpartition view should have unique consumerId.");
        return getSubpartitionMemoryDataManager(subpartitionId)
                .registerNewConsumer(nettyBasedTierConsumerViewId);
    }

    /**
     * Close this {@link MemoryTierProducerAgentWriter}, it means no data will be appended to
     * memory.
     */
    @Override
    public void close() {}

    public boolean isConsumerRegistered(int subpartitionId) {
        int numConsumers = subpartitionViewOperationsMap.get(subpartitionId).size();
        if (isBroadcastOnly) {
            return numConsumers == numSubpartitions;
        }
        return numConsumers > 0;
    }

    // ------------------------------------
    //      Callback for subpartition
    // ------------------------------------

    @Override
    public MemorySegment requestBufferFromPool(int subpartitionId) {
        return tieredStoreMemoryManager.requestMemorySegmentBlocking(TierType.IN_MEM);
    }

    @Override
    public void onDataAvailable(
            int subpartitionId,
            Collection<NettyBasedTierConsumerViewId> nettyBasedTierConsumerViewIds) {
        Map<NettyBasedTierConsumerViewId, NettyBasedTierConsumerView> consumerViewMap =
                subpartitionViewOperationsMap.get(subpartitionId);
        nettyBasedTierConsumerViewIds.forEach(
                consumerId -> {
                    NettyBasedTierConsumerView nettyBasedTierConsumerView =
                            consumerViewMap.get(consumerId);
                    if (nettyBasedTierConsumerView != null) {
                        nettyBasedTierConsumerView.notifyDataAvailable();
                    }
                });
    }

    @Override
    public void onConsumerReleased(
            int subpartitionId, NettyBasedTierConsumerViewId nettyBasedTierConsumerViewId) {
        subpartitionViewOperationsMap.get(subpartitionId).remove(nettyBasedTierConsumerViewId);
        getSubpartitionMemoryDataManager(subpartitionId)
                .releaseConsumer(nettyBasedTierConsumerViewId);
    }

    // ------------------------------------
    //           Internal Method
    // ------------------------------------

    private SubpartitionMemoryDataManager getSubpartitionMemoryDataManager(int targetChannel) {
        return subpartitionMemoryDataManagers[targetChannel];
    }

    public SubpartitionSegmentIndexTracker getSegmentIndexTracker() {
        return subpartitionSegmentIndexTracker;
    }

    public SubpartitionMemoryDataManager[] getSubpartitionMemoryDataManagers() {
        return subpartitionMemoryDataManagers;
    }
}
