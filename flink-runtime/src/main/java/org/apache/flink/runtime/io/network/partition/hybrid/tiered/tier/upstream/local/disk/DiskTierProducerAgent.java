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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.local.disk;

import org.apache.flink.runtime.io.network.api.EndOfSegmentEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TierType;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.common.StorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.common.SubpartitionSegmentIndexTracker;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.common.SubpartitionSegmentIndexTrackerImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.common.TierProducerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.common.CacheFlushManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.common.file.PartitionFileManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.common.file.PartitionFileReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.common.file.PartitionFileType;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.service.NettyBasedTierConsumer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.service.NettyBasedTierConsumerView;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.service.NettyBasedTierConsumerViewId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.service.NettyBasedTierConsumerViewImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.service.NettyServiceViewProvider;
import org.apache.flink.util.ExceptionUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.ADD_SEGMENT_ID_EVENT;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.common.TieredStoreUtils.DATA_FILE_SUFFIX;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** The DataManager of LOCAL file. */
public class DiskTierProducerAgent implements TierProducerAgent, NettyServiceViewProvider {

    public static final int BROADCAST_CHANNEL = 0;

    private final int tierIndex;

    private final ResultPartitionID resultPartitionID;

    private final Path dataFilePath;

    private final float minReservedDiskSpaceFraction;

    private final boolean isBroadcastOnly;

    /** Record the last assigned consumerId for each subpartition. */
    private final NettyBasedTierConsumerViewId[] lastNettyBasedTierConsumerViewIds;

    private final PartitionFileReader partitionFileReader;

    private volatile boolean isReleased;

    private final int[] numSubpartitionEmitBytes;

    private final SubpartitionSegmentIndexTracker segmentIndexTracker;

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
            StorageMemoryManager storageMemoryManager,
            BufferCompressor bufferCompressor,
            CacheFlushManager cacheFlushManager) {
        this.tierIndex = tierIndex;
        this.resultPartitionID = resultPartitionID;
        this.dataFilePath = Paths.get(dataFileBasePath + DATA_FILE_SUFFIX);
        this.minReservedDiskSpaceFraction = minReservedDiskSpaceFraction;
        this.isBroadcastOnly = isBroadcastOnly;
        this.lastNettyBasedTierConsumerViewIds = new NettyBasedTierConsumerViewId[numSubpartitions];
        this.partitionFileReader =
                partitionFileManager.createPartitionFileReader(PartitionFileType.PRODUCER_MERGE);

        this.numSubpartitionEmitBytes = new int[numSubpartitions];
        this.segmentIndexTracker =
                new SubpartitionSegmentIndexTrackerImpl(numSubpartitions, isBroadcastOnly);
        this.diskCacheManager =
                new DiskCacheManager(
                        tierIndex,
                        isBroadcastOnly ? 1 : numSubpartitions,
                        networkBufferSize,
                        storageMemoryManager,
                        cacheFlushManager,
                        bufferCompressor,
                        partitionFileManager);
    }

    @Override
    public NettyBasedTierConsumerView createNettyBasedTierConsumerView(
            int subpartitionId, BufferAvailabilityListener availabilityListener)
            throws IOException {
        // If data file is not readable, throw PartitionNotFoundException to mark this result
        // partition failed. Otherwise, the partition data is not regenerated, so failover can not
        // recover the job.
        if (!Files.isReadable(dataFilePath)) {
            throw new PartitionNotFoundException(resultPartitionID);
        }
        // if broadcastOptimize is enabled, map every subpartitionId to the special broadcast
        // channel.
        subpartitionId = isBroadcastOnly ? BROADCAST_CHANNEL : subpartitionId;

        NettyBasedTierConsumerViewImpl diskTierReaderView =
                new NettyBasedTierConsumerViewImpl(availabilityListener);
        NettyBasedTierConsumerViewId lastNettyBasedTierConsumerViewId =
                lastNettyBasedTierConsumerViewIds[subpartitionId];
        // assign a unique id for each consumer, now it is guaranteed by the value that is one
        // higher than the last consumerId's id field.
        NettyBasedTierConsumerViewId nettyBasedTierConsumerViewId =
                NettyBasedTierConsumerViewId.newId(lastNettyBasedTierConsumerViewId);
        lastNettyBasedTierConsumerViewIds[subpartitionId] = nettyBasedTierConsumerViewId;
        NettyBasedTierConsumer diskConsumer =
                partitionFileReader.registerTierReader(
                        subpartitionId, nettyBasedTierConsumerViewId, diskTierReaderView);
        diskTierReaderView.setConsumer(diskConsumer);
        return diskTierReaderView;
    }

    @Override
    public boolean canStoreNextSegment(int consumerId) {
        File filePath = dataFilePath.toFile();
        return filePath.getUsableSpace()
                > (long) (filePath.getTotalSpace() * minReservedDiskSpaceFraction);
    }

    @Override
    public boolean hasCurrentSegment(int subpartitionId, int segmentIndex) {
        return getSegmentIndexTracker().hasCurrentSegment(subpartitionId, segmentIndex);
    }

    @Override
    public TierType getTierType() {
        return TierType.IN_DISK;
    }

    @Override
    public int getTierIndex() {
        return tierIndex;
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
    public void startSegment(int consumerId, int segmentId) {
        segmentIndexTracker.addSubpartitionSegmentIndex(consumerId, segmentId);
    }

    @Override
    public boolean write(int consumerId, Buffer finishedBuffer) throws IOException {
        boolean isLastBufferInSegment = false;
        numSubpartitionEmitBytes[consumerId] += finishedBuffer.readableBytes();
        if (numSubpartitionEmitBytes[consumerId] >= numBytesInASegment) {
            isLastBufferInSegment = true;
            numSubpartitionEmitBytes[consumerId] = 0;
        }
        if (isLastBufferInSegment) {
            emitBuffer(finishedBuffer, consumerId, false);
            emitEndOfSegmentEvent(consumerId);
        } else {
            emitBuffer(finishedBuffer, consumerId, isLastBufferInSegment);
        }
        return isLastBufferInSegment;
    }

    private void emitEndOfSegmentEvent(int targetChannel) {
        try {
            diskCacheManager.appendSegmentEvent(
                    EventSerializer.toSerializedEvent(EndOfSegmentEvent.INSTANCE),
                    targetChannel,
                    ADD_SEGMENT_ID_EVENT);
        } catch (IOException e) {
            ExceptionUtils.rethrow(e, "Failed to emitEndOfSegmentEvent");
        }
    }

    private void emitBuffer(
            Buffer finishedBuffer, int targetSubpartition, boolean isLastBufferInSegment)
            throws IOException {
        diskCacheManager.append(finishedBuffer, targetSubpartition, isLastBufferInSegment);
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

    public SubpartitionSegmentIndexTracker getSegmentIndexTracker() {
        return segmentIndexTracker;
    }
}
