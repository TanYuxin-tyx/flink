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
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.PartitionFileWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.SubpartitionSegmentIdTracker;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.SubpartitionSegmentIdTrackerImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierProducerAgent;

import java.util.Arrays;

/** The DataManager of DFS. */
public class RemoteTierProducerAgent implements TierProducerAgent {

    private final int numBytesPerSegment;

    // Record the byte number currently written to each sub partition.
    private final int[] numSubpartitionEmitBytes;

    private final SubpartitionSegmentIdTracker segmentIndexTracker;

    private final RemoteCacheManager cacheDataManager;

    private final int[] subpartitionLastestSegmentId;

    public RemoteTierProducerAgent(
            int numSubpartitions,
            int numBytesPerSegment,
            boolean isBroadcastOnly,
            TieredStorageMemoryManager storageMemoryManager,
            PartitionFileWriter partitionFileWriter) {
        this.numBytesPerSegment = numBytesPerSegment;
        this.segmentIndexTracker =
                new SubpartitionSegmentIdTrackerImpl(numSubpartitions, isBroadcastOnly);
        this.cacheDataManager =
                new RemoteCacheManager(
                        isBroadcastOnly ? 1 : numSubpartitions,
                        storageMemoryManager,
                        partitionFileWriter);
        this.numSubpartitionEmitBytes = new int[numSubpartitions];
        this.subpartitionLastestSegmentId = new int[numSubpartitions];
        Arrays.fill(numSubpartitionEmitBytes, 0);
    }

    @Override
    public boolean tryStartNewSegment(
            TieredStorageSubpartitionId subpartitionId,
            int segmentId,
            boolean forceUseCurrentTier) {
        if (!segmentIndexTracker.hasCurrentSegment(subpartitionId, segmentId)) {
            segmentIndexTracker.addSegmentIndex(subpartitionId, segmentId);
            cacheDataManager.startSegment(subpartitionId.getSubpartitionId(), segmentId);
        }
        subpartitionLastestSegmentId[subpartitionId.getSubpartitionId()] = segmentId;
        // The remote storage tier should always be able to start a new segment.
        return true;
    }

    @Override
    public void release() {
        getRemoteCacheManager().release();
        getSegmentIndexTracker().release();
    }

    @Override
    public boolean tryWrite(int consumerId, Buffer finishedBuffer) {
        if (numSubpartitionEmitBytes[consumerId] != 0
                && numSubpartitionEmitBytes[consumerId] + finishedBuffer.readableBytes()
                        > numBytesPerSegment) {
            cacheDataManager.finishSegment(consumerId, subpartitionLastestSegmentId[consumerId]);
            numSubpartitionEmitBytes[consumerId] = 0;
            return false;
        }
        numSubpartitionEmitBytes[consumerId] += finishedBuffer.readableBytes();
        emitBuffer(finishedBuffer, consumerId);
        return true;
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

    public SubpartitionSegmentIdTracker getSegmentIndexTracker() {
        return segmentIndexTracker;
    }
}
