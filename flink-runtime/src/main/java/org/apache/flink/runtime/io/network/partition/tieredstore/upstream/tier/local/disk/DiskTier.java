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

package org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk;

import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.CheckpointedResultSubpartition;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.TieredStoreConfiguration;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.EndOfSegmentEventBuilder;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.StorageTier;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.SubpartitionSegmentIndexTracker;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierReader;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierReaderView;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierReaderViewId;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierWriter;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreMemoryManager;
import org.apache.flink.runtime.metrics.TimerGauge;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.SEGMENT_EVENT;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** The DataManager of LOCAL file. */
public class DiskTier implements TierWriter, StorageTier {

    private static final Logger LOG = LoggerFactory.getLogger(DiskTier.class);

    public static final int BROADCAST_CHANNEL = 0;

    public static final String DATA_FILE_SUFFIX = ".store.data";

    private final int numSubpartitions;

    private final int networkBufferSize;

    private final ResultPartitionID resultPartitionID;

    private final TieredStoreMemoryManager tieredStoreMemoryManager;

    private final Path dataFilePath;

    private final long minDiskReserveBytes;

    private final boolean isBroadcastOnly;

    private final RegionBufferIndexTracker regionBufferIndexTracker;

    private final BufferCompressor bufferCompressor;

    /** Record the last assigned consumerId for each subpartition. */
    private final TierReaderViewId[] lastTierReaderViewIds;

    private final DiskReaderManager diskReaderManager;

    private DiskCacheManager diskCacheManager;

    private final SubpartitionSegmentIndexTracker segmentIndexTracker;

    // TODO, Make this configurable.
    private int numBytesInASegment = 4 * 1024 * 1024; // 4 M

    private volatile boolean isReleased;

    private volatile boolean isClosed;

    public DiskTier(
            int numSubpartitions,
            int networkBufferSize,
            ResultPartitionID resultPartitionID,
            TieredStoreMemoryManager tieredStoreMemoryManager,
            String dataFileBasePath,
            long minDiskReserveBytes,
            boolean isBroadcastOnly,
            @Nullable BufferCompressor bufferCompressor,
            BatchShuffleReadBufferPool readBufferPool,
            ScheduledExecutorService readIOExecutor,
            TieredStoreConfiguration storeConfiguration) {
        this.numSubpartitions = numSubpartitions;
        this.networkBufferSize = networkBufferSize;
        this.resultPartitionID = resultPartitionID;
        this.dataFilePath = new File(dataFileBasePath + DATA_FILE_SUFFIX).toPath();
        this.minDiskReserveBytes = minDiskReserveBytes;
        this.isBroadcastOnly = isBroadcastOnly;
        this.tieredStoreMemoryManager = tieredStoreMemoryManager;
        this.bufferCompressor = bufferCompressor;
        this.regionBufferIndexTracker =
                new RegionBufferIndexTrackerImpl(isBroadcastOnly ? 1 : numSubpartitions);
        this.lastTierReaderViewIds = new TierReaderViewId[numSubpartitions];
        this.diskReaderManager =
                new DiskReaderManager(
                        readBufferPool,
                        readIOExecutor,
                        regionBufferIndexTracker,
                        dataFilePath,
                        SubpartitionDiskReaderImpl.Factory.INSTANCE,
                        storeConfiguration,
                        this::isLastRecordInSegment);
        this.segmentIndexTracker =
                new SubpartitionSegmentIndexTracker(numSubpartitions, isBroadcastOnly);
    }

    @Override
    public void setup() throws IOException {
        this.diskCacheManager =
                new DiskCacheManager(
                        isBroadcastOnly ? 1 : numSubpartitions,
                        networkBufferSize,
                        tieredStoreMemoryManager,
                        regionBufferIndexTracker,
                        dataFilePath,
                        bufferCompressor);
    }

    boolean isLastRecordInSegment(int subpartitionId, int bufferIndex) {
        return diskCacheManager.isLastBufferInSegment(subpartitionId, bufferIndex);
    }

    @Override
    public void emit(
            ByteBuffer record,
            int targetSubpartition,
            Buffer.DataType dataType,
            boolean isBroadcast,
            boolean isLastRecordInSegment,
            boolean isEndOfPartition,
            long segmentIndex)
            throws IOException {
        segmentIndexTracker.addSubpartitionSegmentIndex(targetSubpartition, segmentIndex);
        if (isLastRecordInSegment && !isEndOfPartition) {
            emit(record, targetSubpartition, dataType, false);
            // Send the EndOfSegmentEvent
            ByteBuffer endOfSegment =
                    EndOfSegmentEventBuilder.buildEndOfSegmentEvent(segmentIndex + 1L);
            emit(endOfSegment, targetSubpartition, SEGMENT_EVENT, true);
        } else {
            emit(record, targetSubpartition, dataType, isLastRecordInSegment);
        }
    }

    private void emit(
            ByteBuffer record,
            int targetSubpartition,
            Buffer.DataType dataType,
            boolean isLastRecordInSegment)
            throws IOException {
        diskCacheManager.append(record, targetSubpartition, dataType, isLastRecordInSegment);
    }

    /**
     * In the local tier, only one memory data manager and only one file disk data manager are used,
     * and the subpartitionId is not used. So return directly.
     */
    @Override
    public TierWriter createPartitionTierWriter() {
        return this;
    }

    @Override
    public TierReaderView createSubpartitionTierReaderView(
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

        SubpartitionDiskReaderView subpartitionDiskReaderView =
                new SubpartitionDiskReaderView(availabilityListener);
        TierReaderViewId lastTierReaderViewId = lastTierReaderViewIds[subpartitionId];
        // assign a unique id for each consumer, now it is guaranteed by the value that is one
        // higher than the last consumerId's id field.
        TierReaderViewId tierReaderViewId = TierReaderViewId.newId(lastTierReaderViewId);
        lastTierReaderViewIds[subpartitionId] = tierReaderViewId;
        TierReader diskReader =
                diskReaderManager.registerNewConsumer(
                        subpartitionId, tierReaderViewId, subpartitionDiskReaderView);
        subpartitionDiskReaderView.setDiskReader(diskReader);
        return subpartitionDiskReaderView;
    }

    @Override
    public boolean canStoreNextSegment(int subpartitionId) {
        return dataFilePath.toFile().getUsableSpace() > minDiskReserveBytes;
    }

    @Override
    public int getNewSegmentSize() {
        return numBytesInASegment;
    }

    @Override
    public void setNumBytesInASegment(int numBytesInASegment) {
        this.numBytesInASegment = numBytesInASegment;
    }

    @Override
    public boolean hasCurrentSegment(int subpartitionId, long segmentIndex) {
        return segmentIndexTracker.hasCurrentSegment(subpartitionId, segmentIndex);
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
            diskReaderManager.release();
            checkNotNull(diskCacheManager).release();
            segmentIndexTracker.release();
            isReleased = true;
        }
    }

    @Override
    public void setOutputMetrics(OutputMetrics tieredStoreOutputMetrics) {
        checkNotNull(diskCacheManager).setOutputMetrics(tieredStoreOutputMetrics);
    }

    @Override
    public void setTimerGauge(TimerGauge timerGauge) {
        // nothing to do
    }

    @Override
    public void alignedBarrierTimeout(long checkpointId) throws IOException {
        // Nothing to do
    }

    @Override
    public void abortCheckpoint(long checkpointId, CheckpointException cause) {
        // Nothing to do
    }

    @Override
    public void flushAll() {
        // Nothing to do
    }

    @Override
    public void flush(int subpartitionIndex) {
        // Nothing to do
    }

    @Override
    public int getNumberOfQueuedBuffers() {
        // Batch shuffle does not need to provide QueuedBuffers information
        return Integer.MIN_VALUE;
    }

    @Override
    public long getSizeOfQueuedBuffersUnsafe() {
        // Batch shuffle does not need to provide QueuedBuffers information
        return Integer.MIN_VALUE;
    }

    @Override
    public int getNumberOfQueuedBuffers(int targetSubpartition) {
        // Batch shuffle does not need to provide QueuedBuffers information
        return Integer.MIN_VALUE;
    }

    @Override
    public void setChannelStateWriter(ChannelStateWriter channelStateWriter) {
        // Batch shuffle doesn't support to set channel state writer
    }

    @Override
    public CheckpointedResultSubpartition getCheckpointedSubpartition(int subpartitionIndex) {
        // Batch shuffle doesn't support checkpoint
        return null;
    }

    @Override
    public void finishReadRecoveredState(boolean notifyAndBlockOnCompletion) throws IOException {
        // Batch shuffle doesn't support state
    }

    @Override
    public void onConsumedSubpartition(int subpartitionIndex) {
        // Batch shuffle doesn't support onConsumedSubpartition
    }

    @Override
    public CompletableFuture<Void> getAllDataProcessedFuture() {
        // Batch shuffle doesn't support getAllDataProcessedFuture
        return null;
    }

    @Override
    public void onSubpartitionAllDataProcessed(int subpartition) {
        // Batch shuffle doesn't support onSubpartitionAllDataProcessed
    }
}
