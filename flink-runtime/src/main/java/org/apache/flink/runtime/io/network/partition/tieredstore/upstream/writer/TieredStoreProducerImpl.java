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

package org.apache.flink.runtime.io.network.partition.tieredstore.upstream.writer;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.CheckpointedResultSubpartition;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.StorageTier;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierWriter;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreProducer;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.DiskTier;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.memory.MemoryTier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * This is a common entrypoint of the emitted records. These records will be transferred to the
 * appropriate {@link TierWriter}.
 */
public class TieredStoreProducerImpl implements TieredStoreProducer {

    private static final Logger LOG = LoggerFactory.getLogger(TieredStoreProducerImpl.class);

    private final StorageTier[] tierDataGates;

    private final TierWriter[] tierWriters;

    // Record the newest segment index belonged to each sub partition.
    private final long[] subpartitionSegmentIndexes;

    // Record the byte number currently written to each sub partition.
    private final int[] numSubpartitionEmitBytes;

    // Record the index of writer currently used by each sub partition.
    private final int[] subpartitionWriterIndex;

    private final boolean isBroadcastOnly;

    private final int numSubpartitions;

    public TieredStoreProducerImpl(
            StorageTier[] tierDataGates, int numSubpartitions, boolean isBroadcastOnly)
            throws IOException {
        this.tierDataGates = tierDataGates;
        this.subpartitionSegmentIndexes = new long[numSubpartitions];
        this.numSubpartitionEmitBytes = new int[numSubpartitions];
        this.subpartitionWriterIndex = new int[numSubpartitions];
        this.tierWriters = new TierWriter[tierDataGates.length];
        this.isBroadcastOnly = isBroadcastOnly;
        this.numSubpartitions = numSubpartitions;

        Arrays.fill(subpartitionSegmentIndexes, 0);
        Arrays.fill(numSubpartitionEmitBytes, 0);
        Arrays.fill(subpartitionWriterIndex, -1);
        for (int i = 0; i < tierDataGates.length; i++) {
            tierWriters[i] = tierDataGates[i].createPartitionTierWriter();
        }
    }

    @VisibleForTesting
    @Override
    public void setNumBytesInASegment(int numBytesInASegment) {
        for (int i = 0; i < tierDataGates.length; i++) {
            tierDataGates[i].setNumBytesInASegment(numBytesInASegment);
        }
    }

    @Override
    public void emit(
            ByteBuffer record,
            int targetSubpartition,
            Buffer.DataType dataType,
            boolean isBroadcast,
            boolean isEndOfPartition)
            throws IOException {
        List<WriterAndSegmentIndex> writerAndSegmentIndexes =
                selectTieredWriterAndGetRegionIndexes(
                        record.remaining(), targetSubpartition, isBroadcast);
        for (WriterAndSegmentIndex writerAndSegmentIndex : writerAndSegmentIndexes) {
            long segmentIndex = writerAndSegmentIndex.getSegmentIndex();
            boolean isLastRecord = writerAndSegmentIndex.isLastRecordInSegment();
            int subpartitionId = writerAndSegmentIndex.getSubpartitionId();
            int writerIndex = writerAndSegmentIndex.getWriterIndex();
            tierWriters[writerIndex].emit(
                    record.duplicate(),
                    subpartitionId,
                    dataType,
                    isBroadcast,
                    isLastRecord,
                    isEndOfPartition,
                    segmentIndex);
        }
    }

    // Choose right tiered writers.
    public List<WriterAndSegmentIndex> selectTieredWriterAndGetRegionIndexes(
            int numRecordBytes, int targetSubpartition, boolean isBroadcast) throws IOException {
        List<WriterAndSegmentIndex> writerAndSegmentIndexes = new ArrayList<>();
        if (isBroadcast && !isBroadcastOnly) {
            for (int i = 0; i < numSubpartitions; ++i) {
                writerAndSegmentIndexes.add(getTieredWriterAndGetSegmentIndex(numRecordBytes, i));
            }
        } else {
            writerAndSegmentIndexes.add(
                    getTieredWriterAndGetSegmentIndex(numRecordBytes, targetSubpartition));
        }
        return writerAndSegmentIndexes;
    }

    private WriterAndSegmentIndex getTieredWriterAndGetSegmentIndex(
            int numRecordBytes, int targetSubpartition) throws IOException {

        // Each record needs to get the following information
        int writerIndex;
        boolean isLastRecordInSegment;
        long segmentIndex;

        // For the record that haven't selected a gate to emit
        if (subpartitionWriterIndex[targetSubpartition] == -1) {
            writerIndex = chooseGate(targetSubpartition);
            subpartitionWriterIndex[targetSubpartition] = writerIndex;
            checkState(numSubpartitionEmitBytes[targetSubpartition] == 0);
            numSubpartitionEmitBytes[targetSubpartition] += numRecordBytes;
            if (numSubpartitionEmitBytes[targetSubpartition]
                    >= tierDataGates[writerIndex].getNewSegmentSize()) {
                isLastRecordInSegment = true;
                segmentIndex = subpartitionSegmentIndexes[targetSubpartition];
                ++subpartitionSegmentIndexes[targetSubpartition];
                clearInfoOfSelectGate(targetSubpartition);
            } else {
                isLastRecordInSegment = false;
                segmentIndex = subpartitionSegmentIndexes[targetSubpartition];
            }
        }
        // For the record that already selected a gate to emit
        else {
            int currentWriterIndex = subpartitionWriterIndex[targetSubpartition];
            checkState(currentWriterIndex != -1);
            numSubpartitionEmitBytes[targetSubpartition] += numRecordBytes;
            if (numSubpartitionEmitBytes[targetSubpartition]
                    >= tierDataGates[currentWriterIndex].getNewSegmentSize()) {
                writerIndex = currentWriterIndex;
                isLastRecordInSegment = true;
                segmentIndex = subpartitionSegmentIndexes[targetSubpartition];
                ++subpartitionSegmentIndexes[targetSubpartition];
                clearInfoOfSelectGate(targetSubpartition);
            } else {
                writerIndex = currentWriterIndex;
                isLastRecordInSegment = false;
                segmentIndex = subpartitionSegmentIndexes[targetSubpartition];
            }
        }

        return new WriterAndSegmentIndex(
                writerIndex, isLastRecordInSegment, segmentIndex, targetSubpartition);
    }

    private int chooseGate(int targetSubpartition) throws IOException {
        if (tierDataGates.length == 1) {
            return 0;
        }
        // only for test case Memory and Disk
        if (tierDataGates.length == 2
                && tierDataGates[0] instanceof MemoryTier
                && tierDataGates[1] instanceof DiskTier) {
            if (!isBroadcastOnly && tierDataGates[0].canStoreNextSegment(targetSubpartition)) {
                return 0;
            }
            return 1;
        }
        for (int tierGateIndex = 0; tierGateIndex < tierDataGates.length; ++tierGateIndex) {
            StorageTier tierDataGate = tierDataGates[tierGateIndex];
            if (isBroadcastOnly && tierDataGate instanceof MemoryTier) {
                continue;
            }
            if (tierDataGates[tierGateIndex].canStoreNextSegment(targetSubpartition)) {
                return tierGateIndex;
            }
        }
        throw new IOException("All gates are full, cannot select the writer of gate");
    }

    private void clearInfoOfSelectGate(int targetSubpartition) {
        numSubpartitionEmitBytes[targetSubpartition] = 0;
        subpartitionWriterIndex[targetSubpartition] = -1;
    }

    public void release() {
        for (int i = 0; i < tierWriters.length; i++) {
            tierWriters[i].release();
            tierWriters[i] = null;
        }
        for (int i = 0; i < tierDataGates.length; i++) {
            tierDataGates[i].release();
            tierDataGates[i] = null;
        }
    }

    public void close() {
        Arrays.stream(tierWriters).forEach(TierWriter::close);
        Arrays.stream(tierDataGates).forEach(StorageTier::close);
    }

    @Override
    public void alignedBarrierTimeout(long checkpointId) throws IOException {
        for (StorageTier storageTier : tierDataGates) {
            storageTier.alignedBarrierTimeout(checkpointId);
        }
    }

    @Override
    public void abortCheckpoint(long checkpointId, CheckpointException cause) {
        for (StorageTier storageTier : tierDataGates) {
            storageTier.abortCheckpoint(checkpointId, cause);
        }
    }

    @Override
    public void flushAll() {
        for (StorageTier storageTier : tierDataGates) {
            storageTier.flushAll();
        }
    }

    @Override
    public void flush(int subpartitionIndex) {
        for (StorageTier storageTier : tierDataGates) {
            storageTier.flush(subpartitionIndex);
        }
    }

    @Override
    public int getNumberOfQueuedBuffers() {
        for (StorageTier storageTier : tierDataGates) {
            int numberOfQueuedBuffers = storageTier.getNumberOfQueuedBuffers();
            if (numberOfQueuedBuffers != Integer.MIN_VALUE) {
                return numberOfQueuedBuffers;
            }
        }
        return 0;
    }

    @Override
    public long getSizeOfQueuedBuffersUnsafe() {
        for (StorageTier storageTier : tierDataGates) {
            long sizeOfQueuedBuffersUnsafe = storageTier.getSizeOfQueuedBuffersUnsafe();
            if (sizeOfQueuedBuffersUnsafe != Integer.MIN_VALUE) {
                return sizeOfQueuedBuffersUnsafe;
            }
        }
        return 0;
    }

    @Override
    public int getNumberOfQueuedBuffers(int targetSubpartition) {
        for (StorageTier storageTier : tierDataGates) {
            int numberOfQueuedBuffers = storageTier.getNumberOfQueuedBuffers(targetSubpartition);
            if (numberOfQueuedBuffers != Integer.MIN_VALUE) {
                return numberOfQueuedBuffers;
            }
        }
        return 0;
    }

    @Override
    public void setChannelStateWriter(ChannelStateWriter channelStateWriter) {
        for (StorageTier storageTier : tierDataGates) {
            storageTier.setChannelStateWriter(channelStateWriter);
        }
    }

    @Override
    public CheckpointedResultSubpartition getCheckpointedSubpartition(int subpartitionIndex) {
        for (StorageTier storageTier : tierDataGates) {
            CheckpointedResultSubpartition checkpointedSubpartition =
                    storageTier.getCheckpointedSubpartition(subpartitionIndex);
            if (checkpointedSubpartition != null) {
                return checkpointedSubpartition;
            }
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public void finishReadRecoveredState(boolean notifyAndBlockOnCompletion) throws IOException {
        for (StorageTier storageTier : tierDataGates) {
            storageTier.finishReadRecoveredState(notifyAndBlockOnCompletion);
        }
    }

    @Override
    public void onConsumedSubpartition(int subpartitionIndex) {
        for (StorageTier storageTier : tierDataGates) {
            storageTier.onConsumedSubpartition(subpartitionIndex);
        }
    }

    @Override
    public CompletableFuture<Void> getAllDataProcessedFuture() {
        for (StorageTier storageTier : tierDataGates) {
            CompletableFuture<Void> allDataProcessedFuture =
                    storageTier.getAllDataProcessedFuture();
            if (allDataProcessedFuture != null) {
                return allDataProcessedFuture;
            }
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public void onSubpartitionAllDataProcessed(int subpartition) {
        for (StorageTier storageTier : tierDataGates) {
            storageTier.onSubpartitionAllDataProcessed(subpartition);
        }
    }
}
