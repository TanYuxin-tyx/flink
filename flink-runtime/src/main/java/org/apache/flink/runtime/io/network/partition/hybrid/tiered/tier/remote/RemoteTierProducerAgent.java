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
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.PartitionFileWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.SubpartitionSegmentIdTracker;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.SubpartitionSegmentIdTrackerImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageResourceRegistry;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierProducerAgent;

import java.util.Arrays;

import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageUtils.useNewBufferRecyclerAndCompressBuffer;

/** The DataManager of DFS. */
public class RemoteTierProducerAgent implements TierProducerAgent {

    private final int numSubpartitions;

    private final int numBytesPerSegment;

    // Record the byte number currently written to each sub partition.
    private final int[] numSubpartitionEmitBytes;

    private final SubpartitionSegmentIdTracker segmentIndexTracker;

    private final RemoteCacheManager cacheDataManager;

    private final BufferCompressor bufferCompressor;

    private final TieredStorageMemoryManager memoryManager;

    private final int[] subpartitionSegmentIds;

    public RemoteTierProducerAgent(
            TieredStoragePartitionId partitionId,
            int numSubpartitions,
            int numBytesPerSegment,
            boolean isBroadcastOnly,
            BufferCompressor bufferCompressor,
            PartitionFileWriter partitionFileWriter,
            TieredStorageMemoryManager memoryManager,
            TieredStorageResourceRegistry resourceRegistry) {
        this.numSubpartitions = numSubpartitions;
        this.numBytesPerSegment = numBytesPerSegment;
        this.memoryManager = memoryManager;
        this.bufferCompressor = bufferCompressor;
        this.segmentIndexTracker =
                new SubpartitionSegmentIdTrackerImpl(numSubpartitions, isBroadcastOnly);
        this.cacheDataManager =
                new RemoteCacheManager(
                        isBroadcastOnly ? 1 : numSubpartitions, memoryManager, partitionFileWriter);
        this.numSubpartitionEmitBytes = new int[numSubpartitions];
        this.subpartitionSegmentIds = new int[numSubpartitions];
        Arrays.fill(numSubpartitionEmitBytes, 0);
        resourceRegistry.registerResource(partitionId, this::releaseAllResources);
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
        subpartitionSegmentIds[subpartitionId.getSubpartitionId()] = segmentId;
        // The remote storage tier should always be able to start a new segment.
        return true;
    }

    @Override
    public void release() {}

    @Override
    public boolean tryWrite(int subpartitionId, Buffer buffer) {
        if (numSubpartitionEmitBytes[subpartitionId] != 0
                && numSubpartitionEmitBytes[subpartitionId] + buffer.readableBytes()
                        > numBytesPerSegment) {
            cacheDataManager.finishSegment(subpartitionId, subpartitionSegmentIds[subpartitionId]);
            numSubpartitionEmitBytes[subpartitionId] = 0;
            return false;
        }
        numSubpartitionEmitBytes[subpartitionId] += buffer.readableBytes();
        emitBuffer(
                useNewBufferRecyclerAndCompressBuffer(
                        bufferCompressor, buffer, memoryManager.getOwnerBufferRecycler(this)),
                subpartitionId);
        return true;
    }

    @Override
    public void close() {
        for (int subpartitionId = 0; subpartitionId < numSubpartitions; subpartitionId++) {
            cacheDataManager.finishSegment(subpartitionId, subpartitionSegmentIds[subpartitionId]);
        }
        cacheDataManager.close();
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    private void releaseAllResources() {
        cacheDataManager.release();
        segmentIndexTracker.release();
    }

    private void emitBuffer(Buffer finishedBuffer, int subpartitionId) {
        cacheDataManager.appendBuffer(finishedBuffer, subpartitionId);
    }
}
