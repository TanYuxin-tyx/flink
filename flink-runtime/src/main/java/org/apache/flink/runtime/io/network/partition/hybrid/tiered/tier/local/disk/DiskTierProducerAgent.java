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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.local.disk;

import org.apache.flink.runtime.io.network.api.EndOfSegmentEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStoragePartitionIdAndSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.SegmentSearcher;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.SubpartitionSegmentIdTracker;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.SubpartitionSegmentIdTrackerImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file.PartitionFileManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file.PartitionFileReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file.PartitionFileType;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierProducerAgent;
import org.apache.flink.util.ExceptionUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.END_OF_SEGMENT;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageUtils.DATA_FILE_SUFFIX;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** The DataManager of LOCAL file. */
public class DiskTierProducerAgent implements TierProducerAgent, SegmentSearcher {

    public static final int BROADCAST_CHANNEL = 0;

    private final int tierIndex;

    private final ResultPartitionID resultPartitionID;

    private final Path dataFilePath;

    private final float minReservedDiskSpaceFraction;

    private final boolean isBroadcastOnly;

    /** Record the last assigned consumerId for each subpartition. */
    private final TieredStoragePartitionIdAndSubpartitionId[] lastNettyServiceWriterIds;

    private final PartitionFileReader partitionFileReader;

    private volatile boolean isReleased;

    private final int[] numSubpartitionEmitBytes;

    private final SubpartitionSegmentIdTracker segmentIndexTracker;

    private final DiskCacheManager diskCacheManager;

    // TODO, Make this configurable.
    private int numBytesInASegment = 4 * 1024 * 1024; // 4 M

    private volatile boolean isClosed;

    public DiskTierProducerAgent(
            int tierIndex,
            int numSubpartitions,
            ResultPartitionID resultPartitionID,
            String dataFileBasePath,
            float minReservedDiskSpaceFraction,
            boolean isBroadcastOnly,
            PartitionFileManager partitionFileManager,
            int networkBufferSize,
            BufferCompressor bufferCompressor,
            TieredStorageMemoryManager storageMemoryManager,
            TieredStorageNettyService nettyService) {
        this.tierIndex = tierIndex;
        this.resultPartitionID = resultPartitionID;
        this.dataFilePath = Paths.get(dataFileBasePath + DATA_FILE_SUFFIX);
        this.minReservedDiskSpaceFraction = minReservedDiskSpaceFraction;
        this.isBroadcastOnly = isBroadcastOnly;
        this.lastNettyServiceWriterIds = new TieredStoragePartitionIdAndSubpartitionId[numSubpartitions];
        this.partitionFileReader =
                partitionFileManager.createPartitionFileReader(
                        PartitionFileType.PRODUCER_MERGE, nettyService);

        this.numSubpartitionEmitBytes = new int[numSubpartitions];
        this.segmentIndexTracker =
                new SubpartitionSegmentIdTrackerImpl(numSubpartitions, isBroadcastOnly);
        this.diskCacheManager =
                new DiskCacheManager(
                        tierIndex,
                        isBroadcastOnly ? 1 : numSubpartitions,
                        networkBufferSize,
                        storageMemoryManager,
                        bufferCompressor,
                        partitionFileManager);
    }

    @Override
    public void registerNettyService(int subpartitionId, TieredStoragePartitionIdAndSubpartitionId nettyServiceWriterId)
            throws IOException {
        if (!Files.isReadable(dataFilePath)) {
            throw new PartitionNotFoundException(resultPartitionID);
        }
        partitionFileReader.registerNettyService(subpartitionId, nettyServiceWriterId);
    }

    @Override
    public boolean tryStartNewSegment(
            TieredStorageSubpartitionId subpartitionId,
            int segmentId,
            boolean forceUseCurrentTier) {
        File filePath = dataFilePath.toFile();
        boolean canStartNewSegment =
                filePath.getUsableSpace()
                        > (long) (filePath.getTotalSpace() * minReservedDiskSpaceFraction);
        if (canStartNewSegment || forceUseCurrentTier) {
            segmentIndexTracker.addSegmentIndex(subpartitionId, segmentId);
        }
        return canStartNewSegment || forceUseCurrentTier;
    }

    @Override
    public boolean hasCurrentSegment(TieredStorageSubpartitionId subpartitionId, int segmentIndex) {
        return getSegmentIndexTracker().hasCurrentSegment(subpartitionId, segmentIndex);
    }

    @Override
    public void release() {
        // release is called when release by scheduler, later than close.
        // mainly work :
        // 1. release read scheduler.
        // 2. delete shuffle file.
        // 3. release all data in memory.
        if (!isReleased) {
            partitionFileReader.release();
            getDiskCacheManager().release();
            getSegmentIndexTracker().release();
            isReleased = true;
        }
    }

    @Override
    public boolean write(int consumerId, Buffer finishedBuffer) {
        if (numSubpartitionEmitBytes[consumerId] != 0
                && numSubpartitionEmitBytes[consumerId] + finishedBuffer.readableBytes()
                        > numBytesInASegment) {
            emitEndOfSegmentEvent(consumerId);
            numSubpartitionEmitBytes[consumerId] = 0;
            return false;
        }
        numSubpartitionEmitBytes[consumerId] += finishedBuffer.readableBytes();
        emitBuffer(finishedBuffer, consumerId);
        return true;
    }

    private void emitEndOfSegmentEvent(int targetChannel) {
        try {
            diskCacheManager.appendSegmentEvent(
                    EventSerializer.toSerializedEvent(EndOfSegmentEvent.INSTANCE),
                    targetChannel,
                    END_OF_SEGMENT);
        } catch (IOException e) {
            ExceptionUtils.rethrow(e, "Failed to emitEndOfSegmentEvent");
        }
    }

    private void emitBuffer(Buffer finishedBuffer, int targetSubpartition) {
        diskCacheManager.append(finishedBuffer, targetSubpartition);
    }

    @Override
    public void close() {
        if (!isClosed) {
            // close is called when task is finished or failed.
            checkNotNull(diskCacheManager).close();
            isClosed = true;
        }
    }

    public DiskCacheManager getDiskCacheManager() {
        return diskCacheManager;
    }

    public SubpartitionSegmentIdTracker getSegmentIndexTracker() {
        return segmentIndexTracker;
    }
}
