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

package org.apache.flink.runtime.io.network.partition.store.tier.remote;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.CheckpointedResultSubpartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.store.common.BufferPoolHelper;
import org.apache.flink.runtime.io.network.partition.store.common.StorageTier;
import org.apache.flink.runtime.io.network.partition.store.common.SubpartitionSegmentIndexTracker;
import org.apache.flink.runtime.io.network.partition.store.common.TierReader;
import org.apache.flink.runtime.io.network.partition.store.common.TierReaderView;
import org.apache.flink.runtime.io.network.partition.store.common.TierReaderViewId;
import org.apache.flink.runtime.io.network.partition.store.common.TierWriter;
import org.apache.flink.runtime.io.network.partition.store.tier.local.disk.OutputMetrics;
import org.apache.flink.runtime.metrics.TimerGauge;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.io.network.partition.store.tier.remote.DfsFileWriter.BROADCAST_CHANNEL;

/** The DataManager of DFS. */
public class DfsDataManager implements StorageTier {

    private final int numSubpartitions;

    private final boolean isBroadcastOnly;

    /** Record the last assigned consumerId for each subpartition. */
    private final TierReaderViewId[] lastTierReaderViewIds;

    private final SubpartitionSegmentIndexTracker segmentIndexTracker;

    private final DfsCacheDataManager dfsCacheDataManager;

    // TODO, Make this configurable.
    private int numBytesInASegment = 4 * 1024; // 4 M

    public DfsDataManager(
            JobID jobID,
            int numSubpartitions,
            int networkBufferSize,
            ResultPartitionID resultPartitionID,
            BufferPoolHelper bufferPoolHelper,
            boolean isBroadcastOnly,
            String baseDfsPath,
            @Nullable BufferCompressor bufferCompressor)
            throws IOException {
        this.numSubpartitions = numSubpartitions;
        this.isBroadcastOnly = isBroadcastOnly;
        this.lastTierReaderViewIds = new TierReaderViewId[numSubpartitions];
        this.segmentIndexTracker =
                new SubpartitionSegmentIndexTracker(numSubpartitions, isBroadcastOnly);
        this.dfsCacheDataManager =
                new DfsCacheDataManager(
                        jobID,
                        resultPartitionID,
                        isBroadcastOnly ? 1 : numSubpartitions,
                        networkBufferSize,
                        isBroadcastOnly,
                        baseDfsPath,
                        bufferPoolHelper,
                        bufferCompressor);
    }

    @Override
    public void setup() throws IOException {}

    @Override
    public TierWriter createPartitionTierWriter() throws IOException {
        return new DfsFileWriter(
                numSubpartitions, isBroadcastOnly, segmentIndexTracker, dfsCacheDataManager);
    }

    @Override
    public TierReaderView createSubpartitionTierReaderView(
            int subpartitionId, BufferAvailabilityListener availabilityListener)
            throws IOException {
        // if broadcastOptimize is enabled, map every subpartitionId to the special broadcast
        // channel.
        subpartitionId = isBroadcastOnly ? BROADCAST_CHANNEL : subpartitionId;

        DfsFileReaderView dfsFileReader = new DfsFileReaderView(availabilityListener);
        TierReaderViewId lastTierReaderViewId = lastTierReaderViewIds[subpartitionId];
        // assign a unique id for each consumer, now it is guaranteed by the value that is one
        // higher than the last consumerId's id field.
        TierReaderViewId tierReaderViewId = TierReaderViewId.newId(lastTierReaderViewId);
        lastTierReaderViewIds[subpartitionId] = tierReaderViewId;
        TierReader dfsDataView =
                dfsCacheDataManager.registerNewConsumer(
                        subpartitionId, tierReaderViewId, dfsFileReader);

        dfsFileReader.setDfsDataView(dfsDataView);
        return dfsFileReader;
    }

    @Override
    public boolean canStoreNextSegment(int subpartitionId) {
        return true;
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
    public void setOutputMetrics(OutputMetrics tieredStoreOutputMetrics) {
        dfsCacheDataManager.setOutputMetrics(tieredStoreOutputMetrics);
    }

    @Override
    public void setTimerGauge(TimerGauge timerGauge) {
        // nothing to do
    }

    @Override
    public void close() {}

    @Override
    public void release() {
        segmentIndexTracker.release();
    }

    @VisibleForTesting
    @Override
    public Path getBaseSubpartitionPath(int subpartitionId) {
        return dfsCacheDataManager.getBaseSubpartitionPath(subpartitionId);
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
