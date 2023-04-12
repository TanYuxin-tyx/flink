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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.local.memory;

import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.TierType;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TierStorage;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TierStorageWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageWriterFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoreMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.NettyBasedTierConsumer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.NettyBasedTierConsumerView;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.NettyBasedTierConsumerViewId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.NettyBasedTierConsumerViewImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.NettyBasedTierConsumerViewProvider;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** The DataManager of LOCAL file. */
public class MemoryTierStorage implements TierStorage, NettyBasedTierConsumerViewProvider {

    public static final int BROADCAST_CHANNEL = 0;

    private final int numSubpartitions;

    private final TieredStoreMemoryManager tieredStoreMemoryManager;

    private final boolean isBroadcastOnly;

    /** Record the last assigned consumerId for each subpartition. */
    private final NettyBasedTierConsumerViewId[] lastNettyBasedTierConsumerViewIds;

    private final TieredStorageWriterFactory tieredStorageWriterFactory;

    private TierStorageWriter memoryWriter;

    public static final int MEMORY_TIER_SEGMENT_BYTES = 10 * 32 * 1024;

    private final int bufferNumberInSegment = MEMORY_TIER_SEGMENT_BYTES / 32 / 1024;

    private volatile boolean isReleased;

    public MemoryTierStorage(
            int numSubpartitions,
            TieredStoreMemoryManager tieredStoreMemoryManager,
            boolean isBroadcastOnly,
            TieredStorageWriterFactory tieredStorageWriterFactory) {
        this.numSubpartitions = numSubpartitions;
        this.isBroadcastOnly = isBroadcastOnly;
        this.tieredStoreMemoryManager = tieredStoreMemoryManager;
        this.lastNettyBasedTierConsumerViewIds = new NettyBasedTierConsumerViewId[numSubpartitions];
        this.tieredStorageWriterFactory = tieredStorageWriterFactory;
    }

    @Override
    public void setup() throws IOException {
        this.memoryWriter = tieredStorageWriterFactory.createTierStorageWriter(TierType.IN_MEM);
        this.memoryWriter.setup();
    }

    /**
     * In the local tier, only one memory data manager and only one file disk data manager are used,
     * and the subpartitionId is not used. So return directly.
     */
    @Override
    public TierStorageWriter createPartitionTierWriter() {
        return memoryWriter;
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
                ((MemoryTierStorageWriter) checkNotNull(memoryWriter))
                        .registerNewConsumer(
                                subpartitionId, nettyBasedTierConsumerViewId, memoryReaderView);

        memoryReaderView.setConsumer(memoryConsumer);
        return memoryReaderView;
    }

    @Override
    public boolean canStoreNextSegment(int consumerId) {
        return tieredStoreMemoryManager.numAvailableBuffers(TierType.IN_MEM) > bufferNumberInSegment
                && ((MemoryTierStorageWriter) checkNotNull(memoryWriter))
                        .isConsumerRegistered(consumerId);
    }

    @Override
    public boolean hasCurrentSegment(int subpartitionId, int segmentIndex) {
        return ((MemoryTierStorageWriter) checkNotNull(memoryWriter))
                .getSegmentIndexTracker()
                .hasCurrentSegment(subpartitionId, segmentIndex);
    }

    @Override
    public void release() {
        for (int i = 0; i < numSubpartitions; i++) {
            ((MemoryTierStorageWriter) checkNotNull(memoryWriter))
                    .getSubpartitionMemoryDataManagers()[i].release();
        }

        // release is called when release by scheduler, later than close.
        // mainly work :
        // 1. release read scheduler.
        // 2. delete shuffle file.
        // 3. release all data in memory.

        if (!isReleased) {
            ((MemoryTierStorageWriter) checkNotNull(memoryWriter))
                    .getSegmentIndexTracker()
                    .release();
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
}
