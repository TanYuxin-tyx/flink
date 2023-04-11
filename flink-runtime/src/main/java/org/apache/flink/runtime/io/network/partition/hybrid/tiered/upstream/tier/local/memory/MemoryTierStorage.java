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

import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.TierType;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.NettyBasedTierConsumer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.NettyBasedTierConsumerView;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.NettyBasedTierConsumerViewId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.NettyBasedTierConsumerViewImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.NettyBasedTierConsumerViewProvider;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.SubpartitionSegmentIndexTracker;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.SubpartitionSegmentIndexTrackerImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.TierStorage;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.TierWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.TieredStoreMemoryManager;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** The DataManager of LOCAL file. */
public class MemoryTierStorage implements TierStorage, NettyBasedTierConsumerViewProvider {

    public static final int BROADCAST_CHANNEL = 0;

    private final int numSubpartitions;

    private final int networkBufferSize;

    private final TieredStoreMemoryManager tieredStoreMemoryManager;

    private final boolean isBroadcastOnly;

    private final BufferCompressor bufferCompressor;

    /** Record the last assigned consumerId for each subpartition. */
    private final NettyBasedTierConsumerViewId[] lastNettyBasedTierConsumerViewIds;

    private MemoryTierWriter memoryWriter;

    private final SubpartitionSegmentIndexTracker segmentIndexTracker;

    private int numBytesInASegment = 10 * 32 * 1024;

    private final int bufferNumberInSegment = numBytesInASegment / 32 / 1024;

    private final SubpartitionMemoryDataManager[] subpartitionMemoryDataManagers;

    private volatile boolean isReleased;

    public MemoryTierStorage(
            int numSubpartitions,
            int networkBufferSize,
            TieredStoreMemoryManager tieredStoreMemoryManager,
            boolean isBroadcastOnly,
            @Nullable BufferCompressor bufferCompressor) {
        this.numSubpartitions = numSubpartitions;
        this.networkBufferSize = networkBufferSize;
        this.isBroadcastOnly = isBroadcastOnly;
        this.tieredStoreMemoryManager = tieredStoreMemoryManager;
        this.bufferCompressor = bufferCompressor;
        checkNotNull(bufferCompressor);
        this.lastNettyBasedTierConsumerViewIds = new NettyBasedTierConsumerViewId[numSubpartitions];
        this.segmentIndexTracker =
                new SubpartitionSegmentIndexTrackerImpl(numSubpartitions, isBroadcastOnly);
        this.subpartitionMemoryDataManagers = new SubpartitionMemoryDataManager[numSubpartitions];
    }

    @Override
    public void setup() throws IOException {
        this.memoryWriter =
                new MemoryTierWriter(
                        isBroadcastOnly ? 1 : numSubpartitions,
                        networkBufferSize,
                        tieredStoreMemoryManager,
                        bufferCompressor,
                        segmentIndexTracker,
                        isBroadcastOnly,
                        numSubpartitions,
                        numBytesInASegment,
                        subpartitionMemoryDataManagers);
        this.memoryWriter.setup();
    }

    /**
     * In the local tier, only one memory data manager and only one file disk data manager are used,
     * and the subpartitionId is not used. So return directly.
     */
    @Override
    public TierWriter createPartitionTierWriter() {
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
                checkNotNull(memoryWriter)
                        .registerNewConsumer(
                                subpartitionId, nettyBasedTierConsumerViewId, memoryReaderView);

        memoryReaderView.setConsumer(memoryConsumer);
        return memoryReaderView;
    }

    @Override
    public boolean canStoreNextSegment(int consumerId) {
        return tieredStoreMemoryManager.numAvailableBuffers(TierType.IN_MEM) > bufferNumberInSegment
                && memoryWriter.isConsumerRegistered(consumerId);
    }

    @Override
    public boolean hasCurrentSegment(int subpartitionId, int segmentIndex) {
        return segmentIndexTracker.hasCurrentSegment(subpartitionId, segmentIndex);
    }

    @Override
    public void release() {
        for (int i = 0; i < numSubpartitions; i++) {
            subpartitionMemoryDataManagers[i].release();
        }
        // release is called when release by scheduler, later than close.
        // mainly work :
        // 1. release read scheduler.
        // 2. delete shuffle file.
        // 3. release all data in memory.
        if (!isReleased) {
            segmentIndexTracker.release();
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
