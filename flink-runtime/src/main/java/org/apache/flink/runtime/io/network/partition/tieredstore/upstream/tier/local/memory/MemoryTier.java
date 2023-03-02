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

package org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.memory;

import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.CheckpointedResultSubpartition;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.TieredStoreMode;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.StorageTier;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.SubpartitionSegmentIndexTracker;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierReader;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierReaderView;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierReaderViewId;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierWriter;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreMemoryManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.OutputMetrics;
import org.apache.flink.runtime.metrics.TimerGauge;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** The DataManager of LOCAL file. */
public class MemoryTier implements StorageTier {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryTier.class);

    public static final int BROADCAST_CHANNEL = 0;

    private final int numSubpartitions;

    private final int networkBufferSize;

    private final TieredStoreMemoryManager tieredStoreMemoryManager;

    private final boolean isBroadcastOnly;

    private final BufferCompressor bufferCompressor;

    /** Record the last assigned consumerId for each subpartition. */
    private final TierReaderViewId[] lastTierReaderViewIds;

    private MemoryWriter memoryWriter;

    private final SubpartitionSegmentIndexTracker segmentIndexTracker;

    private int numBytesInASegment = 10 * 32 * 1024;

    private final int bufferNumberInSegment = numBytesInASegment / 32 / 1024;

    private volatile boolean isReleased;

    private volatile boolean isClosed;

    public MemoryTier(
            int numSubpartitions,
            int networkBufferSize,
            TieredStoreMemoryManager tieredStoreMemoryManager,
            boolean isBroadcastOnly,
            @Nullable BufferCompressor bufferCompressor) {
        this.numSubpartitions = numSubpartitions;
        this.networkBufferSize = networkBufferSize;
        this.isBroadcastOnly = isBroadcastOnly;
        this.tieredStoreMemoryManager = tieredStoreMemoryManager;
        this.bufferCompressor = bufferCompressor;
        checkNotNull(bufferCompressor);
        this.lastTierReaderViewIds = new TierReaderViewId[numSubpartitions];
        this.segmentIndexTracker =
                new SubpartitionSegmentIndexTracker(numSubpartitions, isBroadcastOnly);
    }

    @Override
    public void setup() throws IOException {
        this.memoryWriter =
                new MemoryWriter(
                        isBroadcastOnly ? 1 : numSubpartitions,
                        networkBufferSize,
                        tieredStoreMemoryManager,
                        bufferCompressor,
                        segmentIndexTracker,
                        isBroadcastOnly,
                        numSubpartitions,
                        numBytesInASegment);
        this.memoryWriter.setup();
    }

    /**
     * In the local tier, only one memory data manager and only one file disk data manager are used,
     * and the subpartitionId is not used. So return directly.
     */
    @Override
    public TierWriter createPartitionTierWriter() {
        return memoryWriter;
    }

    @Override
    public TierReaderView createSubpartitionTierReaderView(
            int subpartitionId, BufferAvailabilityListener availabilityListener)
            throws IOException {
        // if broadcastOptimize is enabled, map every subpartitionId to the special broadcast
        // channel.
        subpartitionId = isBroadcastOnly ? BROADCAST_CHANNEL : subpartitionId;

        SubpartitionMemoryReaderView memoryReaderView =
                new SubpartitionMemoryReaderView(availabilityListener);
        TierReaderViewId lastTierReaderViewId = lastTierReaderViewIds[subpartitionId];
        checkMultipleConsumerIsAllowed(lastTierReaderViewId);
        // assign a unique id for each consumer, now it is guaranteed by the value that is one
        // higher than the last consumerId's id field.
        TierReaderViewId tierReaderViewId = TierReaderViewId.newId(lastTierReaderViewId);
        lastTierReaderViewIds[subpartitionId] = tierReaderViewId;

        TierReader memoryReader =
                checkNotNull(memoryWriter)
                        .registerNewConsumer(subpartitionId, tierReaderViewId, memoryReaderView);

        memoryReaderView.setMemoryDataView(memoryReader);
        return memoryReaderView;
    }

    @Override
    public boolean canStoreNextSegment(int subpartitionId) {
        return tieredStoreMemoryManager.numAvailableBuffers(TieredStoreMode.TieredType.IN_MEM)
                        > bufferNumberInSegment
                && memoryWriter.isConsumerRegistered(subpartitionId);
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
            segmentIndexTracker.release();
            isReleased = true;
        }
    }

    @Override
    public void setOutputMetrics(OutputMetrics tieredStoreOutputMetrics) {
        checkNotNull(memoryWriter).setOutputMetrics(tieredStoreOutputMetrics);
    }

    @Override
    public void setTimerGauge(TimerGauge timerGauge) {
        // nothing to do
    }

    private static void checkMultipleConsumerIsAllowed(TierReaderViewId lastTierReaderViewId) {
        checkState(lastTierReaderViewId == null, "Memory Tier does not support multiple consumers");
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
