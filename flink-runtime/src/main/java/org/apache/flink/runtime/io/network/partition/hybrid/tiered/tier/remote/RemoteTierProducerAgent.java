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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.remote;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.shuffle.TierType;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.SubpartitionSegmentIndexTracker;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.SubpartitionSegmentIndexTrackerImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TierProducerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.CacheFlushManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file.PartitionFileManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file.PartitionFileType;

import java.io.IOException;
import java.util.Arrays;

/** The DataManager of DFS. */
public class RemoteTierProducerAgent implements TierProducerAgent {

    // Record the byte number currently written to each sub partition.
    private final int[] numSubpartitionEmitBytes;

    private final SubpartitionSegmentIndexTracker segmentIndexTracker;

    private final RemoteCacheManager cacheDataManager;

    // TODO, Make this configurable.
    private int numBytesInASegment = 4 * 1024 * 1024; // 4 M

    private final int[] subpartitionLastestSegmentId;

    private int tierIndex;

    public RemoteTierProducerAgent(
            int numSubpartitions,
            boolean isBroadcastOnly,
            int networkBufferSize,
            TieredStorageMemoryManager storageMemoryManager,
            CacheFlushManager cacheFlushManager,
            BufferCompressor bufferCompressor,
            PartitionFileManager partitionFileManager) {
        this.segmentIndexTracker =
                new SubpartitionSegmentIndexTrackerImpl(numSubpartitions, isBroadcastOnly);
        this.cacheDataManager =
                new RemoteCacheManager(
                        isBroadcastOnly ? 1 : numSubpartitions,
                        networkBufferSize,
                        storageMemoryManager,
                        cacheFlushManager,
                        bufferCompressor,
                        partitionFileManager.createPartitionFileWriter(
                                PartitionFileType.PRODUCER_HASH));
        this.numSubpartitionEmitBytes = new int[numSubpartitions];
        this.subpartitionLastestSegmentId = new int[numSubpartitions];
        Arrays.fill(numSubpartitionEmitBytes, 0);
    }

    @Override
    public boolean canStoreNextSegment(int consumerId) {
        return true;
    }

    @Override
    public TierType getTierType() {
        return TierType.IN_REMOTE;
    }

    // Only for test, this should be removed in production code.
    public void setTierIndex(int tierIndex) {
        this.tierIndex = tierIndex;
    }

    @Override
    public int getTierIndex() {
        return tierIndex;
    }

    @Override
    public void release() {
        getRemoteCacheManager().release();
        getSegmentIndexTracker().release();
    }

    @Override
    public void startSegment(int consumerId, int segmentId) {
        if (!segmentIndexTracker.hasCurrentSegment(consumerId, segmentId)) {
            segmentIndexTracker.addSubpartitionSegmentIndex(consumerId, segmentId);
            cacheDataManager.startSegment(consumerId, segmentId);
        }
        subpartitionLastestSegmentId[consumerId] = segmentId;
    }

    @Override
    public boolean write(int consumerId, Buffer finishedBuffer) throws IOException {
        boolean isLastBufferInSegment = false;
        numSubpartitionEmitBytes[consumerId] += finishedBuffer.readableBytes();
        if (numSubpartitionEmitBytes[consumerId] >= numBytesInASegment) {
            isLastBufferInSegment = true;
            numSubpartitionEmitBytes[consumerId] = 0;
        }
        emitBuffer(finishedBuffer, consumerId);

        if (isLastBufferInSegment) {
            cacheDataManager.finishSegment(consumerId, subpartitionLastestSegmentId[consumerId]);
        }
        return isLastBufferInSegment;
    }

    private void emitBuffer(Buffer finishedBuffer, int targetSubpartition) {
        cacheDataManager.appendBuffer(finishedBuffer, targetSubpartition);
    }

    @Override
    public void close() {
        for (int index = 0; index < subpartitionLastestSegmentId.length; ++index) {
            cacheDataManager.finishSegment(index, subpartitionLastestSegmentId[index]);
        }
        cacheDataManager.close();
    }

    public RemoteCacheManager getRemoteCacheManager() {
        return cacheDataManager;
    }

    public SubpartitionSegmentIndexTracker getSegmentIndexTracker() {
        return segmentIndexTracker;
    }
}
