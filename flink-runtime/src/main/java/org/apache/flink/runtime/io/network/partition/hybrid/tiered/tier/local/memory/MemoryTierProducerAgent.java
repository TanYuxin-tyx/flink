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
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.CreditBasedBufferQueueView;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.CreditBasedShuffleViewId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.ProducerNettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.SegmentSearcher;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.SubpartitionSegmentIdTracker;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.SubpartitionSegmentIdTrackerImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierProducerAgent;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.END_OF_SEGMENT;
import static org.apache.flink.util.Preconditions.checkState;

/** The DataManager of LOCAL file. */
public class MemoryTierProducerAgent
        implements TierProducerAgent, MemoryTierProducerAgentOperation, SegmentSearcher {

    public static final int BROADCAST_CHANNEL = 0;

    private final int tierIndex;

    private final int numSubpartitions;

    private final TieredStorageMemoryManager storageMemoryManager;

    private final boolean isBroadcastOnly;

    /** Record the last assigned consumerId for each subpartition. */
    private final CreditBasedShuffleViewId[] lastCreditBasedShuffleViewIds;

    public static final int MEMORY_TIER_SEGMENT_BYTES = 10 * 32 * 1024;

    private final int bufferNumberInSegment = MEMORY_TIER_SEGMENT_BYTES / 32 / 1024;

    private volatile boolean isReleased;

    // Record the byte number currently written to each sub partition.
    private final int[] numSubpartitionEmitBytes;

    /**
     * Each element of the list is all views of the subpartition corresponding to its index, which
     * are stored in the form of a map that maps consumer id to its subpartition view.
     */
    private final List<Map<CreditBasedShuffleViewId, CreditBasedBufferQueueView>> subpartitionViewOperationsMap;

    private final SubpartitionMemoryDataManager[] subpartitionMemoryDataManagers;

    private final SubpartitionSegmentIdTracker subpartitionSegmentIdTracker;

    public MemoryTierProducerAgent(
            int tierIndex,
            int numSubpartitions,
            TieredStorageMemoryManager storageMemoryManager,
            boolean isBroadcastOnly,
            BufferCompressor bufferCompressor,
            int bufferSize,
            ProducerNettyService nettyService) {
        this.tierIndex = tierIndex;
        this.numSubpartitions = numSubpartitions;
        this.isBroadcastOnly = isBroadcastOnly;
        this.storageMemoryManager = storageMemoryManager;
        this.lastCreditBasedShuffleViewIds = new CreditBasedShuffleViewId[numSubpartitions];

        this.numSubpartitionEmitBytes = new int[numSubpartitions];
        Arrays.fill(numSubpartitionEmitBytes, 0);
        this.subpartitionViewOperationsMap = new ArrayList<>(numSubpartitions);
        this.subpartitionMemoryDataManagers = new SubpartitionMemoryDataManager[numSubpartitions];
        this.subpartitionSegmentIdTracker =
                new SubpartitionSegmentIdTrackerImpl(numSubpartitions, isBroadcastOnly);
        for (int subpartitionId = 0; subpartitionId < numSubpartitions; ++subpartitionId) {
            subpartitionMemoryDataManagers[subpartitionId] =
                    new SubpartitionMemoryDataManager(
                            subpartitionId, bufferSize, bufferCompressor, this, nettyService);
            subpartitionViewOperationsMap.add(new ConcurrentHashMap<>());
        }
    }

    @Override
    public CreditBasedBufferQueueView registerNettyService(
            int subpartitionId, BufferAvailabilityListener availabilityListener) {
        // if broadcastOptimize is enabled, map every subpartitionId to the special broadcast
        // channel.
        subpartitionId = isBroadcastOnly ? BROADCAST_CHANNEL : subpartitionId;

        // NettyServiceViewImpl memoryReaderView = new NettyServiceViewImpl(availabilityListener);
        CreditBasedShuffleViewId lastCreditBasedShuffleViewId = lastCreditBasedShuffleViewIds[subpartitionId];
        checkMultipleConsumerIsAllowed(lastCreditBasedShuffleViewId);
        // assign a unique id for each consumer, now it is guaranteed by the value that is one
        // higher than the last consumerId's id field.
        CreditBasedShuffleViewId creditBasedShuffleViewId = CreditBasedShuffleViewId.newId(
                lastCreditBasedShuffleViewId);
        lastCreditBasedShuffleViewIds[subpartitionId] = creditBasedShuffleViewId;
        CreditBasedBufferQueueView creditBasedBufferQueueView =
                subpartitionMemoryDataManagers[subpartitionId].registerNettyService(
                        creditBasedShuffleViewId, availabilityListener);
        CreditBasedBufferQueueView oldView =
                subpartitionViewOperationsMap
                        .get(subpartitionId)
                        .put(creditBasedShuffleViewId, creditBasedBufferQueueView);
        Preconditions.checkState(
                oldView == null, "Each subpartition view should have unique consumerId.");
        return creditBasedBufferQueueView;
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
                isConsumerRegistered(subpartitionId)
                        && (storageMemoryManager.getMaxNonReclaimableBuffers(this)
                                        - storageMemoryManager.numOwnerRequestedBuffer(this))
                                > bufferNumberInSegment;
        if (canStartNewSegment || forceUseCurrentTier) {
            subpartitionSegmentIdTracker.addSegmentIndex(subpartitionId, segmentId);
        }
        return canStartNewSegment || forceUseCurrentTier;
    }

    @Override
    public boolean hasCurrentSegment(TieredStorageSubpartitionId subpartitionId, int segmentIndex) {
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

    private static void checkMultipleConsumerIsAllowed(CreditBasedShuffleViewId lastCreditBasedShuffleViewId) {
        checkState(
                lastCreditBasedShuffleViewId == null, "Memory Tier does not support multiple consumers");
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

    public boolean isConsumerRegistered(int subpartitionId) {
        int numConsumers = subpartitionViewOperationsMap.get(subpartitionId).size();
        if (isBroadcastOnly) {
            return numConsumers == numSubpartitions;
        }
        return numConsumers > 0;
    }

    public boolean isConsumerRegistered(TieredStorageSubpartitionId subpartitionId) {
        int numConsumers =
                subpartitionViewOperationsMap.get(subpartitionId.getSubpartitionId()).size();
        if (isBroadcastOnly) {
            return numConsumers == numSubpartitions;
        }
        return numConsumers > 0;
    }

    // ------------------------------------
    //      Callback for subpartition
    // ------------------------------------

    @Override
    public void onDataAvailable(
            int subpartitionId, Collection<CreditBasedShuffleViewId> creditBasedShuffleViewIds) {
        Map<CreditBasedShuffleViewId, CreditBasedBufferQueueView> consumerViewMap =
                subpartitionViewOperationsMap.get(subpartitionId);
        creditBasedShuffleViewIds.forEach(
                consumerId -> {
                    CreditBasedBufferQueueView creditBasedBufferQueueView = consumerViewMap.get(consumerId);
                    if (creditBasedBufferQueueView != null) {
                        creditBasedBufferQueueView.notifyDataAvailable();
                    }
                });
    }

    @Override
    public void onConsumerReleased(int subpartitionId, CreditBasedShuffleViewId creditBasedShuffleViewId) {
        subpartitionViewOperationsMap.get(subpartitionId).remove(creditBasedShuffleViewId);
        getSubpartitionMemoryDataManager(subpartitionId).releaseConsumer(creditBasedShuffleViewId);
    }

    // ------------------------------------
    //           Internal Method
    // ------------------------------------

    private SubpartitionMemoryDataManager getSubpartitionMemoryDataManager(int targetChannel) {
        return subpartitionMemoryDataManagers[targetChannel];
    }

    public SubpartitionSegmentIdTracker getSegmentIndexTracker() {
        return subpartitionSegmentIdTracker;
    }

    public SubpartitionMemoryDataManager[] getSubpartitionMemoryDataManagers() {
        return subpartitionMemoryDataManagers;
    }
}
