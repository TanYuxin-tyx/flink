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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.tier.local.disk;

import org.apache.flink.runtime.io.network.api.EndOfSegmentEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.TierType;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.CacheFlushManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.NettyBasedTierConsumer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.NettyBasedTierConsumerView;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.NettyBasedTierConsumerViewImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.NettyBasedTierConsumerViewProvider;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.SubpartitionSegmentIndexTracker;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.SubpartitionSegmentIndexTrackerImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.TierReaderViewId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.TierStorage;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.TierWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.TieredStoreMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.file.PartitionFileManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.file.PartitionFileReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.file.PartitionFileType;
import org.apache.flink.util.ExceptionUtils;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.SEGMENT_EVENT;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.TieredStoreUtils.DATA_FILE_SUFFIX;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** The DataManager of LOCAL file. */
public class DiskTierWriter implements TierStorage, TierWriter, NettyBasedTierConsumerViewProvider {

    public static final int BROADCAST_CHANNEL = 0;

    private final int numSubpartitions;

    private final int networkBufferSize;

    private final ResultPartitionID resultPartitionID;

    private final TieredStoreMemoryManager tieredStoreMemoryManager;

    private final CacheFlushManager cacheFlushManager;

    private final Path dataFilePath;

    private final float minReservedDiskSpaceFraction;

    private final boolean isBroadcastOnly;

    private final BufferCompressor bufferCompressor;

    /** Record the last assigned consumerId for each subpartition. */
    private final TierReaderViewId[] lastTierReaderViewIds;

    private final PartitionFileReader partitionFileReader;

    // Record the byte number currently written to each sub partition.
    private final int[] numSubpartitionEmitBytes;

    private final SubpartitionSegmentIndexTracker segmentIndexTracker;

    private final PartitionFileManager partitionFileManager;

    private DiskCacheManager diskCacheManager;

    // TODO, Make this configurable.
    private int numBytesInASegment = 4 * 1024 * 1024; // 4 M

    private volatile boolean isReleased;

    private volatile boolean isClosed;

    public DiskTierWriter(
            int numSubpartitions,
            int networkBufferSize,
            ResultPartitionID resultPartitionID,
            TieredStoreMemoryManager tieredStoreMemoryManager,
            CacheFlushManager cacheFlushManager,
            String dataFileBasePath,
            float minReservedDiskSpaceFraction,
            boolean isBroadcastOnly,
            @Nullable BufferCompressor bufferCompressor,
            PartitionFileManager partitionFileManager) {
        this.numSubpartitions = numSubpartitions;
        this.networkBufferSize = networkBufferSize;
        this.resultPartitionID = resultPartitionID;
        this.dataFilePath = Paths.get(dataFileBasePath + DATA_FILE_SUFFIX);
        this.minReservedDiskSpaceFraction = minReservedDiskSpaceFraction;
        this.isBroadcastOnly = isBroadcastOnly;
        this.tieredStoreMemoryManager = tieredStoreMemoryManager;
        this.cacheFlushManager = cacheFlushManager;
        this.bufferCompressor = bufferCompressor;
        this.numSubpartitionEmitBytes = new int[numSubpartitions];
        Arrays.fill(numSubpartitionEmitBytes, 0);
        this.lastTierReaderViewIds = new TierReaderViewId[numSubpartitions];
        this.segmentIndexTracker =
                new SubpartitionSegmentIndexTrackerImpl(numSubpartitions, isBroadcastOnly);
        this.partitionFileManager = partitionFileManager;
        this.partitionFileReader =
                partitionFileManager.createPartitionFileReader(PartitionFileType.PRODUCER_MERGE);
    }

    @Override
    public void setup() throws IOException {
        this.diskCacheManager =
                new DiskCacheManager(
                        isBroadcastOnly ? 1 : numSubpartitions,
                        networkBufferSize,
                        tieredStoreMemoryManager,
                        cacheFlushManager,
                        bufferCompressor,
                        partitionFileManager);
    }

    @Override
    public boolean emit(
            int targetSubpartition, Buffer finishedBuffer, boolean isEndOfPartition, int segmentId)
            throws IOException {
        boolean isLastBufferInSegment = false;
        numSubpartitionEmitBytes[targetSubpartition] += finishedBuffer.readableBytes();
        if (numSubpartitionEmitBytes[targetSubpartition] >= numBytesInASegment) {
            isLastBufferInSegment = true;
            numSubpartitionEmitBytes[targetSubpartition] = 0;
        }

        segmentIndexTracker.addSubpartitionSegmentIndex(targetSubpartition, segmentId);
        if (isLastBufferInSegment && !isEndOfPartition) {
            emitBuffer(finishedBuffer, targetSubpartition, false);
            emitEndOfSegmentEvent(segmentId, targetSubpartition);
        } else {
            emitBuffer(finishedBuffer, targetSubpartition, isLastBufferInSegment);
        }
        return isLastBufferInSegment;
    }

    private void emitEndOfSegmentEvent(int segmentId, int targetChannel) {
        try {
            diskCacheManager.appendSegmentEvent(
                    EventSerializer.toSerializedEvent(EndOfSegmentEvent.INSTANCE),
                    targetChannel,
                    SEGMENT_EVENT);
        } catch (IOException e) {
            ExceptionUtils.rethrow(e, "Failed to emitEndOfSegmentEvent");
        }
    }

    private void emitBuffer(
            Buffer finishedBuffer, int targetSubpartition, boolean isLastBufferInSegment)
            throws IOException {
        diskCacheManager.append(finishedBuffer, targetSubpartition, isLastBufferInSegment);
    }

    /**
     * In the local tier, only one memory data manager and only one file disk data manager are used,
     * and the subpartitionId is not used. So return directly.
     */
    @Override
    public TierStorage createPartitionTierStorage() {
        return this;
    }

    @Override
    public NettyBasedTierConsumerView createTierReaderView(
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
        TierReaderViewId lastTierReaderViewId = lastTierReaderViewIds[subpartitionId];
        // assign a unique id for each consumer, now it is guaranteed by the value that is one
        // higher than the last consumerId's id field.
        TierReaderViewId tierReaderViewId = TierReaderViewId.newId(lastTierReaderViewId);
        lastTierReaderViewIds[subpartitionId] = tierReaderViewId;
        NettyBasedTierConsumer diskReader =
                partitionFileReader.registerTierReader(
                        subpartitionId, tierReaderViewId, diskTierReaderView);
        diskTierReaderView.setTierReader(diskReader);
        return diskTierReaderView;
    }

    @Override
    public boolean canStoreNextSegment(int subpartitionId) {
        File filePath = dataFilePath.toFile();
        return filePath.getUsableSpace()
                > (long) (filePath.getTotalSpace() * minReservedDiskSpaceFraction);
    }

    @Override
    public boolean hasCurrentSegment(int subpartitionId, int segmentIndex) {
        return segmentIndexTracker.hasCurrentSegment(subpartitionId, segmentIndex);
    }

    @Override
    public TierType getTierType() {
        return TierType.IN_DISK;
    }

    @Override
    public org.apache.flink.core.fs.Path getBaseSubpartitionPath(int subpartitionId) {
        return null;
    }

    @Override
    public void close() {
        if (!isClosed) {
            // close is called when task is finished or failed.
            checkNotNull(diskCacheManager).close();
            isClosed = true;
        }
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
            checkNotNull(diskCacheManager).release();
            segmentIndexTracker.release();
            isReleased = true;
        }
    }
}
