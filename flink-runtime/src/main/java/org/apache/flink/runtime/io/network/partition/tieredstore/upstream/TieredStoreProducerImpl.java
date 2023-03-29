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
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.CheckpointedResultSubpartition;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.cache.BufferAccumulatorImpl;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.cache.CachedBufferContext;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.cache.MemorySegmentAndChannel;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.StorageTier;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierWriter;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreMemoryManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreProducer;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.DiskTier;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.memory.MemoryTier;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * This is a common entrypoint of the emitted records. These records will be transferred to the
 * appropriate {@link TierWriter}.
 */
public class TieredStoreProducerImpl implements TieredStoreProducer {

    private static final Logger LOG = LoggerFactory.getLogger(TieredStoreProducerImpl.class);

    private final StorageTier[] storageTiers;

    private final TierWriter[] tierWriters;

    private final BufferRecycler[] bufferRecyclers;

    private final TieredStoreMode.TieredType[] tieredTypes;

    private final BufferCompressor bufferCompressor;

    private final TieredStoreMemoryManager storeMemoryManager;

    // Record the newest segment index belonged to each sub partition.
    private final int[] subpartitionSegmentIndexes;

    // Record the index of writer currently used by each sub partition.
    private final int[] subpartitionWriterIndex;

    private final boolean isBroadcastOnly;

    private final int numSubpartitions;

    private final BufferAccumulatorImpl bufferAccumulator;

    public TieredStoreProducerImpl(
            StorageTier[] storageTiers,
            TieredStoreMode.TieredType[] tieredTypes,
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
        this.tieredTypes = tieredTypes;
        this.bufferCompressor = bufferCompressor;
        this.storeMemoryManager = storeMemoryManager;
        this.isBroadcastOnly = isBroadcastOnly;
        this.numSubpartitions = numSubpartitions;
        this.bufferAccumulator =
                new BufferAccumulatorImpl(
                        numSubpartitions,
                        bufferSize,
                        storeMemoryManager,
                        this::notifyFinishedBuffer);
        this.bufferRecyclers = new BufferRecycler[storageTiers.length];

        Arrays.fill(subpartitionSegmentIndexes, 0);
        Arrays.fill(subpartitionWriterIndex, -1);
        for (int i = 0; i < storageTiers.length; i++) {
            tierWriters[i] = storageTiers[i].createPartitionTierWriter();
            TieredStoreMode.TieredType tieredType = tieredTypes[i];
            bufferRecyclers[i] = buffer -> storeMemoryManager.recycleBuffer(buffer, tieredType);
        }

        checkState(storageTiers.length == tieredTypes.length, "Wrong number of tiers.");
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
                // TODO, remove the useless isBroadcast argument.
                bufferAccumulator.emit(record.duplicate(), i, dataType, false, isEndOfPartition);
            }
        } else {
            bufferAccumulator.emit(record, targetSubpartition, dataType, false, isEndOfPartition);
        }
    }

    @Override
    public void emitBuffers(
            List<MemorySegmentAndChannel> finishedSegments,
            boolean isBroadcast,
            boolean isEndOfPartition)
            throws IOException {
        for (MemorySegmentAndChannel finishedSegment : finishedSegments) {
            emitFinishedBuffer(finishedSegment, isBroadcast, isEndOfPartition);
        }
    }

    private void emitFinishedBuffer(
            MemorySegmentAndChannel finishedSegment, boolean isBroadcast, boolean isEndOfPartition)
            throws IOException {
        int targetSubpartition = finishedSegment.getChannelIndex();
        int tierIndex = subpartitionWriterIndex[targetSubpartition];
        // For the first buffer
        if (tierIndex == -1) {
            tierIndex = chooseStorageTierIndex(targetSubpartition);
            subpartitionWriterIndex[targetSubpartition] = tierIndex;
        }

        int segmentIndex = subpartitionSegmentIndexes[targetSubpartition];
        if (finishedSegment.getDataType().isBuffer()) {
            storeMemoryManager.decNumRequestedBuffer(TieredStoreMode.TieredType.IN_CACHE);
            storeMemoryManager.incNumRequestedBuffer(tieredTypes[tierIndex]);
        }
        Buffer finishedBuffer =
                new NetworkBuffer(
                        finishedSegment.getBuffer(),
                        finishedSegment.getDataType().isBuffer()
                                ? bufferRecyclers[tierIndex]
                                : FreeingBufferRecycler.INSTANCE,
                        finishedSegment.getDataType(),
                        finishedSegment.getDataSize());
        boolean isLastBufferInSegment =
                tierWriters[tierIndex].emit(
                        targetSubpartition,
                        compressBufferIfPossible(finishedBuffer),
                        isBroadcast,
                        isEndOfPartition,
                        segmentIndex);
        if (finishedSegment.getDataType().isBuffer()) {
            storeMemoryManager.checkNeedTriggerFlushCachedBuffers();
        }
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

    private void notifyFinishedBuffer(CachedBufferContext bufferContext) {
        try {
            emitBuffers(
                    bufferContext.getMemorySegmentAndChannels(),
                    bufferContext.isBroadcast(),
                    bufferContext.isEndOfPartition());
        } catch (IOException e) {
            ExceptionUtils.rethrow(e);
        }
    }

    private Buffer compressBufferIfPossible(Buffer buffer) {
        if (!canBeCompressed(buffer)) {
            return buffer;
        }

        return checkNotNull(bufferCompressor).compressToOriginalBuffer(buffer);
    }

    /**
     * Whether the buffer can be compressed or not. Note that event is not compressed because it is
     * usually small and the size can become even larger after compression.
     */
    private boolean canBeCompressed(Buffer buffer) {
        return bufferCompressor != null && buffer.isBuffer() && buffer.readableBytes() > 0;
    }
}
