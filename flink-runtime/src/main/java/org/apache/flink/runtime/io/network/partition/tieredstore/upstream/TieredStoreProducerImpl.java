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

package org.apache.flink.runtime.io.network.partition.tieredstore.upstream;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.partition.CheckpointedResultSubpartition;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.cache.BufferAccumulator;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.StorageTier;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierWriter;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreMemoryManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreProducer;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.DiskTier;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.memory.MemoryTier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * This is a common entrypoint of the emitted records. These records will be transferred to the
 * appropriate {@link TierWriter}.
 */
public class TieredStoreProducerImpl implements TieredStoreProducer {

    private static final Logger LOG = LoggerFactory.getLogger(TieredStoreProducerImpl.class);

    private final StorageTier[] storageTiers;

    private final TierWriter[] tierWriters;

    // Record the newest segment index belonged to each sub partition.
    private final int[] subpartitionSegmentIndexes;

    // Record the index of writer currently used by each sub partition.
    private final int[] subpartitionWriterIndex;

    private final boolean isBroadcastOnly;

    private final int numSubpartitions;

    private final BufferAccumulator bufferAccumulator;

    public TieredStoreProducerImpl(
            StorageTier[] storageTiers,
            int numSubpartitions,
            int bufferSize,
            @Nullable BufferCompressor bufferCompressor,
            boolean isBroadcastOnly,
            TieredStoreMemoryManager storeMemoryManager)
            throws IOException {
        this.storageTiers = storageTiers;
        this.subpartitionSegmentIndexes = new int[numSubpartitions];
        this.subpartitionWriterIndex = new int[numSubpartitions];
        this.tierWriters = new TierWriter[storageTiers.length];
        this.isBroadcastOnly = isBroadcastOnly;
        this.numSubpartitions = numSubpartitions;
        this.bufferAccumulator =
                new BufferAccumulator(
                        numSubpartitions, bufferSize, bufferCompressor, storeMemoryManager, this);

        Arrays.fill(subpartitionSegmentIndexes, 0);
        Arrays.fill(subpartitionWriterIndex, -1);
        for (int i = 0; i < storageTiers.length; i++) {
            tierWriters[i] = storageTiers[i].createPartitionTierWriter();
        }
    }

    @VisibleForTesting
    @Override
    public void setNumBytesInASegment(int numBytesInASegment) {
        for (TierWriter tierWriter : tierWriters) {
            tierWriter.setNumBytesInASegment(numBytesInASegment);
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

        if (isBroadcast && !isBroadcastOnly) {
            for (int i = 0; i < numSubpartitions; ++i) {
                bufferAccumulator.emitInternal(
                        record.duplicate(), i, dataType, isBroadcast, isEndOfPartition);
            }
        } else {
            bufferAccumulator.emitInternal(
                    record, targetSubpartition, dataType, isBroadcast, isEndOfPartition);
        }
    }

    @Override
    public void emitBuffers(
            int targetSubpartition,
            List<Buffer> finishedBuffers,
            boolean isBroadcast,
            boolean isEndOfPartition)
            throws IOException {
        for (Buffer finishedBuffer : finishedBuffers) {
            emitFinishedBuffer(targetSubpartition, finishedBuffer, isBroadcast, isEndOfPartition);
        }
    }

    private void emitFinishedBuffer(
            int targetSubpartition,
            Buffer finishedBuffer,
            boolean isBroadcast,
            boolean isEndOfPartition)
            throws IOException {
        int tierIndex = subpartitionWriterIndex[targetSubpartition];
        // For the first buffer
        if (tierIndex == -1) {
            tierIndex = chooseStorageTierIndex(targetSubpartition);
            subpartitionWriterIndex[targetSubpartition] = tierIndex;
        }

        int segmentIndex = subpartitionSegmentIndexes[targetSubpartition];
        boolean isLastBufferInSegment =
                tierWriters[tierIndex].emitBuffer(
                        targetSubpartition,
                        finishedBuffer,
                        isBroadcast,
                        isEndOfPartition,
                        segmentIndex);
        if (isLastBufferInSegment) {
            tierIndex = chooseStorageTierIndex(targetSubpartition);
            subpartitionWriterIndex[targetSubpartition] = tierIndex;
            subpartitionSegmentIndexes[targetSubpartition] = (segmentIndex + 1);
        }
    }

    private int chooseStorageTierIndex(int targetSubpartition) throws IOException {
        if (storageTiers.length == 1) {
            return 0;
        }
        // only for test case Memory and Disk
        if (storageTiers.length == 2
                && storageTiers[0] instanceof MemoryTier
                && storageTiers[1] instanceof DiskTier) {
            if (!isBroadcastOnly && storageTiers[0].canStoreNextSegment(targetSubpartition)) {
                return 0;
            }
            return 1;
        }
        for (int tierIndex = 0; tierIndex < storageTiers.length; ++tierIndex) {
            StorageTier storageTier = storageTiers[tierIndex];
            if (isBroadcastOnly && storageTier instanceof MemoryTier) {
                continue;
            }
            if (storageTiers[tierIndex].canStoreNextSegment(targetSubpartition)) {
                return tierIndex;
            }
        }
        throw new IOException("All gates are full, cannot select the writer of gate");
    }

    public void release() {
        Arrays.stream(tierWriters).forEach(TierWriter::release);
        Arrays.stream(storageTiers).forEach(StorageTier::release);
    }

    public void close() {
        Arrays.stream(tierWriters).forEach(TierWriter::close);
        Arrays.stream(storageTiers).forEach(StorageTier::close);
    }

    @Override
    public void alignedBarrierTimeout(long checkpointId) throws IOException {
        for (StorageTier storageTier : storageTiers) {
            storageTier.alignedBarrierTimeout(checkpointId);
        }
    }

    @Override
    public void abortCheckpoint(long checkpointId, CheckpointException cause) {
        for (StorageTier storageTier : storageTiers) {
            storageTier.abortCheckpoint(checkpointId, cause);
        }
    }

    @Override
    public void flushAll() {
        for (StorageTier storageTier : storageTiers) {
            storageTier.flushAll();
        }
    }

    @Override
    public void flush(int subpartitionIndex) {
        for (StorageTier storageTier : storageTiers) {
            storageTier.flush(subpartitionIndex);
        }
    }

    @Override
    public int getNumberOfQueuedBuffers() {
        for (StorageTier storageTier : storageTiers) {
            int numberOfQueuedBuffers = storageTier.getNumberOfQueuedBuffers();
            if (numberOfQueuedBuffers != Integer.MIN_VALUE) {
                return numberOfQueuedBuffers;
            }
        }
        return 0;
    }

    @Override
    public long getSizeOfQueuedBuffersUnsafe() {
        for (StorageTier storageTier : storageTiers) {
            long sizeOfQueuedBuffersUnsafe = storageTier.getSizeOfQueuedBuffersUnsafe();
            if (sizeOfQueuedBuffersUnsafe != Integer.MIN_VALUE) {
                return sizeOfQueuedBuffersUnsafe;
            }
        }
        return 0;
    }

    @Override
    public int getNumberOfQueuedBuffers(int targetSubpartition) {
        for (StorageTier storageTier : storageTiers) {
            int numberOfQueuedBuffers = storageTier.getNumberOfQueuedBuffers(targetSubpartition);
            if (numberOfQueuedBuffers != Integer.MIN_VALUE) {
                return numberOfQueuedBuffers;
            }
        }
        return 0;
    }

    @Override
    public void setChannelStateWriter(ChannelStateWriter channelStateWriter) {
        for (StorageTier storageTier : storageTiers) {
            storageTier.setChannelStateWriter(channelStateWriter);
        }
    }

    @Override
    public CheckpointedResultSubpartition getCheckpointedSubpartition(int subpartitionIndex) {
        for (StorageTier storageTier : storageTiers) {
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
        for (StorageTier storageTier : storageTiers) {
            storageTier.finishReadRecoveredState(notifyAndBlockOnCompletion);
        }
    }

    @Override
    public void onConsumedSubpartition(int subpartitionIndex) {
        for (StorageTier storageTier : storageTiers) {
            storageTier.onConsumedSubpartition(subpartitionIndex);
        }
    }

    @Override
    public CompletableFuture<Void> getAllDataProcessedFuture() {
        for (StorageTier storageTier : storageTiers) {
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
        for (StorageTier storageTier : storageTiers) {
            storageTier.onSubpartitionAllDataProcessed(subpartition);
        }
    }
}
