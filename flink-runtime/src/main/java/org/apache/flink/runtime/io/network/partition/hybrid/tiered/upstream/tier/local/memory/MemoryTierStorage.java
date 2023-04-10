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
import org.apache.flink.runtime.io.network.api.EndOfSegmentEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.TierType;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.NettyBasedTierConsumer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.NettyBasedTierConsumerView;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.SubpartitionSegmentIndexTracker;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.NettyBasedTierConsumerViewId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.TierStorage;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.TieredStoreMemoryManager;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.SEGMENT_EVENT;

/** This class is responsible for managing cached buffers data before flush to local files. */
public class MemoryTierStorage implements TierStorage, MemoryDataWriterOperation {

    private final int numSubpartitions;

    private final SubpartitionMemoryDataManager[] subpartitionMemoryDataManagers;

    private final TieredStoreMemoryManager tieredStoreMemoryManager;

    /**
     * Each element of the list is all views of the subpartition corresponding to its index, which
     * are stored in the form of a map that maps consumer id to its subpartition view.
     */
    private final List<Map<NettyBasedTierConsumerViewId, NettyBasedTierConsumerView>>
            subpartitionViewOperationsMap;

    // Record the byte number currently written to each sub partition.
    private final int[] numSubpartitionEmitBytes;

    private final SubpartitionSegmentIndexTracker subpartitionSegmentIndexTracker;

    private final boolean isBroadcastOnly;

    private final int numTotalConsumers;

    private int numBytesInASegment;

    public MemoryTierStorage(
            int numSubpartitions,
            int bufferSize,
            TieredStoreMemoryManager tieredStoreMemoryManager,
            BufferCompressor bufferCompressor,
            SubpartitionSegmentIndexTracker subpartitionSegmentIndexTracker,
            boolean isBroadcastOnly,
            int numTotalConsumers,
            int numBytesInASegment) {
        this.numSubpartitions = numSubpartitions;
        this.numTotalConsumers = numTotalConsumers;
        this.tieredStoreMemoryManager = tieredStoreMemoryManager;
        this.subpartitionMemoryDataManagers = new SubpartitionMemoryDataManager[numSubpartitions];
        this.subpartitionSegmentIndexTracker = subpartitionSegmentIndexTracker;
        this.isBroadcastOnly = isBroadcastOnly;
        this.numSubpartitionEmitBytes = new int[numSubpartitions];
        Arrays.fill(numSubpartitionEmitBytes, 0);
        this.numBytesInASegment = numBytesInASegment;

        this.subpartitionViewOperationsMap = new ArrayList<>(numSubpartitions);
        for (int subpartitionId = 0; subpartitionId < numSubpartitions; ++subpartitionId) {
            subpartitionMemoryDataManagers[subpartitionId] =
                    new SubpartitionMemoryDataManager(
                            subpartitionId, bufferSize, bufferCompressor, this);
            subpartitionViewOperationsMap.add(new ConcurrentHashMap<>());
        }
    }

    @Override
    public void setup() throws IOException {}

    @Override
    public boolean emit(
            int targetSubpartition,
            Buffer finishedBuffer,
            boolean isEndOfPartition,
            int segmentId) {
        boolean isLastBufferInSegment = false;
        numSubpartitionEmitBytes[targetSubpartition] += finishedBuffer.readableBytes();
        if (numSubpartitionEmitBytes[targetSubpartition] >= numBytesInASegment) {
            isLastBufferInSegment = true;
            numSubpartitionEmitBytes[targetSubpartition] = 0;
        }
        subpartitionSegmentIndexTracker.addSubpartitionSegmentIndex(targetSubpartition, segmentId);
        if (isLastBufferInSegment && !isEndOfPartition) {
            append(finishedBuffer, targetSubpartition);
            // Send the EndOfSegmentEvent
            appendEndOfSegmentEvent(segmentId, targetSubpartition);
        } else {
            append(finishedBuffer, targetSubpartition);
        }
        return isLastBufferInSegment;
    }

    private void appendEndOfSegmentEvent(int segmentId, int targetChannel) {
        try {
            getSubpartitionMemoryDataManager(targetChannel)
                    .appendSegmentEvent(
                            EventSerializer.toSerializedEvent(EndOfSegmentEvent.INSTANCE),
                            SEGMENT_EVENT);
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

    /** Close this {@link MemoryTierStorage}, it means no data will be appended to memory. */
    @Override
    public void close() {}

    /**
     * Release this {@link MemoryTierStorage}, it means all memory taken by this class will recycle.
     */
    @Override
    public void release() {
        for (int i = 0; i < numSubpartitions; i++) {
            getSubpartitionMemoryDataManager(i).release();
        }
    }

    public boolean isConsumerRegistered(int subpartitionId) {
        int numConsumers = subpartitionViewOperationsMap.get(subpartitionId).size();
        if (isBroadcastOnly) {
            return numConsumers == numTotalConsumers;
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
            int subpartitionId, Collection<NettyBasedTierConsumerViewId> nettyBasedTierConsumerViewIds) {
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
    public void onConsumerReleased(int subpartitionId, NettyBasedTierConsumerViewId nettyBasedTierConsumerViewId) {
        subpartitionViewOperationsMap.get(subpartitionId).remove(nettyBasedTierConsumerViewId);
        getSubpartitionMemoryDataManager(subpartitionId).releaseConsumer(nettyBasedTierConsumerViewId);
    }

    // ------------------------------------
    //           Internal Method
    // ------------------------------------

    private SubpartitionMemoryDataManager getSubpartitionMemoryDataManager(int targetChannel) {
        return subpartitionMemoryDataManagers[targetChannel];
    }
}
