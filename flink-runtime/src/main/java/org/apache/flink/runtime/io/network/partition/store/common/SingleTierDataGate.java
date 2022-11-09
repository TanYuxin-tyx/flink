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

package org.apache.flink.runtime.io.network.partition.store.common;

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ChannelStateHolder;
import org.apache.flink.runtime.io.network.partition.CheckpointedResultPartition;
import org.apache.flink.runtime.io.network.partition.store.tier.local.file.OutputMetrics;
import org.apache.flink.runtime.metrics.TimerGauge;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * The gate for a single tiered data. The gate is used to create {@link SingleTierWriter} and {@link
 * SingleTierReader}. The writing and reading data processes happen in the writer and reader.
 */
public interface SingleTierDataGate extends ChannelStateHolder, CheckpointedResultPartition {

    void setup() throws IOException;

    SingleTierWriter createPartitionTierWriter() throws IOException;

    SingleTierReader createSubpartitionTierReader(
            int subpartitionId, BufferAvailabilityListener availabilityListener) throws IOException;

    boolean canStoreNextSegment(int subpartitionId);

    int getNewSegmentSize();

    void setNumBytesInASegment(int numBytesInASegment);

    boolean hasCurrentSegment(int subpartitionId, long segmentIndex);

    void setOutputMetrics(OutputMetrics tieredStoreOutputMetrics);

    void setTimerGauge(TimerGauge timerGauge);

    void close();

    void release();

    default Path getBaseSubpartitionPath(int subpartitionId) {
        return null;
    }

    void alignedBarrierTimeout(long checkpointId) throws IOException;

    void abortCheckpoint(long checkpointId, CheckpointException cause);

    void flushAll();

    void flush(int subpartitionIndex);

    int getNumberOfQueuedBuffers();

    long getSizeOfQueuedBuffersUnsafe();

    int getNumberOfQueuedBuffers(int targetSubpartition);

    void onConsumedSubpartition(int subpartitionIndex);

    CompletableFuture<Void> getAllDataProcessedFuture();

    void onSubpartitionAllDataProcessed(int subpartition);
}
