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

package org.apache.flink.runtime.io.network.partition.tieredstore.upstream.cache;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.tieredstore.TieredStoreMode;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.MemorySegmentAndChannel;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.OutputMetrics;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.StorageTier;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierWriter;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreMemoryManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.DiskTier;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.memory.MemoryTier;
import org.apache.flink.util.ExceptionUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class BufferAccumulatorImpl implements BufferAccumulator {

    private final StorageTier[] storageTiers;

    private final TierWriter[] tierWriters;

    private final TieredStoreMode.TierType[] tierTypes;

    private final boolean isBroadcastOnly;

    private final BufferCompressor bufferCompressor;

    private final HashBasedCachedBuffer cachedBuffer;

    private final TieredStoreMemoryManager storeMemoryManager;

    private final BufferRecycler[] bufferRecyclers;

    /** Records the newest segment index belonged to each subpartition. */
    private final int[] subpartitionSegmentIndexes;

    /** Record the index of tier writer currently used by each subpartition. */
    private final int[] subpartitionWriterIndex;

    @Nullable private OutputMetrics outputMetrics;

    public BufferAccumulatorImpl(
            StorageTier[] storageTiers,
            int numSubpartitions,
            int bufferSize,
            boolean isBroadcastOnly,
            TieredStoreMemoryManager storeMemoryManager,
            @Nullable BufferCompressor bufferCompressor) {
        this.storageTiers = storageTiers;
        this.storeMemoryManager = storeMemoryManager;
        this.bufferCompressor = bufferCompressor;
        this.isBroadcastOnly = isBroadcastOnly;
        this.tierWriters = new TierWriter[storageTiers.length];
        this.subpartitionSegmentIndexes = new int[numSubpartitions];
        this.subpartitionWriterIndex = new int[numSubpartitions];
        this.bufferRecyclers = new BufferRecycler[storageTiers.length];
        this.tierTypes = new TieredStoreMode.TierType[storageTiers.length];

        for (int i = 0; i < storageTiers.length; i++) {
            tierWriters[i] = storageTiers[i].createPartitionTierWriter();
        }

        for (int i = 0; i < storageTiers.length; i++) {
            tierWriters[i] = storageTiers[i].createPartitionTierWriter();
            tierTypes[i] = storageTiers[i].getTierType();
            TieredStoreMode.TierType tierType = tierTypes[i];
            bufferRecyclers[i] = buffer -> storeMemoryManager.recycleBuffer(buffer, tierType);
        }

        this.cachedBuffer =
                new HashBasedCachedBuffer(
                        numSubpartitions, bufferSize, storeMemoryManager, this::emitFinishedBuffer);

        Arrays.fill(subpartitionSegmentIndexes, 0);
        Arrays.fill(subpartitionWriterIndex, -1);
    }

    @Override
    public void receive(
            ByteBuffer record,
            int targetSubpartition,
            Buffer.DataType dataType,
            boolean isEndOfPartition)
            throws IOException {
        cachedBuffer.append(record, targetSubpartition, dataType, isEndOfPartition);
    }

    @Override
    public void emitFinishedBuffer(
            List<MemorySegmentAndChannel> memorySegmentAndChannels, boolean isEndOfPartition) {
        try {
            emitBuffers(memorySegmentAndChannels, isEndOfPartition);
        } catch (IOException e) {
            ExceptionUtils.rethrow(e);
        }
    }

    @Override
    public void setMetricGroup(OutputMetrics metrics) {
        this.outputMetrics = checkNotNull(metrics);
    }

    public void close() {
        Arrays.stream(tierWriters).forEach(TierWriter::close);
        Arrays.stream(storageTiers).forEach(StorageTier::close);
    }

    public void release() {
        Arrays.stream(tierWriters).forEach(TierWriter::release);
        Arrays.stream(storageTiers).forEach(StorageTier::release);
    }

    void emitBuffers(List<MemorySegmentAndChannel> finishedSegments, boolean isEndOfPartition)
            throws IOException {
        for (MemorySegmentAndChannel finishedSegment : finishedSegments) {
            emitFinishedBuffer(finishedSegment, isEndOfPartition);
        }
    }

    private void emitFinishedBuffer(
            MemorySegmentAndChannel finishedSegment, boolean isEndOfPartition) throws IOException {
        int targetSubpartition = finishedSegment.getChannelIndex();
        int tierIndex = subpartitionWriterIndex[targetSubpartition];
        // For the first buffer
        if (tierIndex == -1) {
            tierIndex = chooseStorageTierIndex(targetSubpartition);
            subpartitionWriterIndex[targetSubpartition] = tierIndex;
        }

        int segmentIndex = subpartitionSegmentIndexes[targetSubpartition];
        if (finishedSegment.getDataType().isBuffer()) {
            storeMemoryManager.decNumRequestedBuffer(TieredStoreMode.TierType.IN_CACHE);
            storeMemoryManager.incNumRequestedBuffer(tierTypes[tierIndex]);
        }
        Buffer finishedBuffer =
                new NetworkBuffer(
                        finishedSegment.getBuffer(),
                        finishedSegment.getDataType().isBuffer()
                                ? bufferRecyclers[tierIndex]
                                : FreeingBufferRecycler.INSTANCE,
                        finishedSegment.getDataType(),
                        finishedSegment.getDataSize());
        Buffer compressedBuffer = compressBufferIfPossible(finishedBuffer);
        updateStatistics(compressedBuffer);
        boolean isLastBufferInSegment =
                tierWriters[tierIndex].emit(
                        targetSubpartition, compressedBuffer, isEndOfPartition, segmentIndex);
        storeMemoryManager.checkNeedTriggerFlushCachedBuffers();
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

    private void updateStatistics(Buffer buffer) {
        checkNotNull(outputMetrics).getNumBuffersOut().inc();
        checkNotNull(outputMetrics).getNumBytesOut().inc(buffer.readableBytes());
    }
}
