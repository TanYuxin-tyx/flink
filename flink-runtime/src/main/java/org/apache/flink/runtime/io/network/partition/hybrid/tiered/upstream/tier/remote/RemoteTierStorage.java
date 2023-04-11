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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.tier.remote;

import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.TierType;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.CacheFlushManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.SubpartitionSegmentIndexTracker;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.SubpartitionSegmentIndexTrackerImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.TierStorage;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.TierWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.TieredStoreMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.file.PartitionFileManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.file.PartitionFileType;

import javax.annotation.Nullable;

import java.io.IOException;

/** The DataManager of DFS. */
public class RemoteTierStorage implements TierStorage {

    private final int numSubpartitions;

    private final SubpartitionSegmentIndexTracker segmentIndexTracker;

    private final RemoteCacheManager remoteCacheManager;

    // TODO, Make this configurable.
    private int numBytesInASegment = 4 * 1024; // 4 M

    public RemoteTierStorage(
            int numSubpartitions,
            int networkBufferSize,
            TieredStoreMemoryManager tieredStoreMemoryManager,
            CacheFlushManager cacheFlushManager,
            boolean isBroadcastOnly,
            @Nullable BufferCompressor bufferCompressor,
            PartitionFileManager partitionFileManager) {
        this.numSubpartitions = numSubpartitions;
        this.segmentIndexTracker =
                new SubpartitionSegmentIndexTrackerImpl(numSubpartitions, isBroadcastOnly);
        this.remoteCacheManager =
                new RemoteCacheManager(
                        isBroadcastOnly ? 1 : numSubpartitions,
                        networkBufferSize,
                        tieredStoreMemoryManager,
                        cacheFlushManager,
                        bufferCompressor,
                        partitionFileManager.createPartitionFileWriter(
                                PartitionFileType.PRODUCER_HASH));
    }

    @Override
    public void setup() throws IOException {}

    @Override
    public TierWriter createPartitionTierWriter() {
        return new RemoteTierWriter(
                numSubpartitions, segmentIndexTracker, remoteCacheManager, numBytesInASegment);
    }

    @Override
    public boolean canStoreNextSegment(int consumerId) {
        return true;
    }

    @Override
    public TierType getTierType() {
        return TierType.IN_REMOTE;
    }

    @Override
    public void release() {
        remoteCacheManager.release();
        segmentIndexTracker.release();
        segmentIndexTracker.release();
    }
}
