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

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.CheckpointedResultSubpartition;
import org.apache.flink.runtime.io.network.partition.store.common.SingleTierDataGate;
import org.apache.flink.runtime.io.network.partition.store.common.SingleTierReader;
import org.apache.flink.runtime.io.network.partition.store.common.SingleTierWriter;
import org.apache.flink.runtime.io.network.partition.store.tier.local.file.OutputMetrics;
import org.apache.flink.runtime.metrics.TimerGauge;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/** The DataManager of Remote. */
public class RemoteDataManager implements SingleTierDataGate {
    @Override
    public void setup() throws IOException {}

    @Override
    public SingleTierWriter createPartitionTierWriter() throws IOException {
        return null;
    }

    @Override
    public SingleTierReader createSubpartitionTierReader(
            int subpartitionId, BufferAvailabilityListener availabilityListener)
            throws IOException {
        return null;
    }

    @Override
    public boolean canStoreNextSegment(int subpartitionId) {
        return false;
    }

    @Override
    public int getNewSegmentSize() {
        return 0;
    }

    @Override
    public void setNumBytesInASegment(int numBytesInASegment) {}

    @Override
    public boolean hasCurrentSegment(int subpartitionId, long segmentIndex) {
        return false;
    }

    @Override
    public void setOutputMetrics(OutputMetrics tieredStoreOutputMetrics) {}

    @Override
    public void setTimerGauge(TimerGauge timerGauge) {
        // nothing to do
    }

    @Override
    public void close() {}

    @Override
    public void release() {}

    @Override
    public Path getBaseSubpartitionPath(int subpartitionId) {
        return null;
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
