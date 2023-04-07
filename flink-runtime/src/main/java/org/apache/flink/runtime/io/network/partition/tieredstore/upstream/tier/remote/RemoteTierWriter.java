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

package org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.remote;

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.partition.tieredstore.TierType;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.CacheFlushManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.SubpartitionSegmentIndexTracker;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.SubpartitionSegmentIndexTrackerImpl;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierContainer;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierWriter;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreMemoryManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.file.PartitionFileManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.file.PartitionFileType;

import javax.annotation.Nullable;

import java.io.IOException;

/** The DataManager of DFS. */
public class RemoteTierWriter implements TierWriter {

    private final int numSubpartitions;

    private final SubpartitionSegmentIndexTracker segmentIndexTracker;

    private final RemoteCacheManager remoteCacheManager;

    // TODO, Make this configurable.
    private int numBytesInASegment = 4 * 1024; // 4 M

    public RemoteTierWriter(
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
    public TierContainer createPartitionTierWriter() {
        return new RemoteTierContainer(
                numSubpartitions, segmentIndexTracker, remoteCacheManager, numBytesInASegment);
    }

    @Override
    public boolean canStoreNextSegment(int subpartitionId) {
        return true;
    }

    @Override
    public boolean hasCurrentSegment(int subpartitionId, int segmentIndex) {
        return segmentIndexTracker.hasCurrentSegment(subpartitionId, segmentIndex);
    }

    @Override
    public TierType getTierType() {
        return TierType.IN_REMOTE;
    }

    @Override
    public Path getBaseSubpartitionPath(int subpartitionId) {
        return remoteCacheManager.getBaseSubpartitionPath(subpartitionId);
    }

    @Override
    public void close() {}

    @Override
    public void release() {
        segmentIndexTracker.release();
    }
}
