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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.cache;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.TierType;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.MemorySegmentAndChannel;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.OutputMetrics;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.TierStorage;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.TierWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.TieredStoreMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.TieredStoreProducer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.tier.local.disk.DiskTierStorage;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.tier.local.memory.MemoryTierStorage;
import org.apache.flink.util.ExceptionUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The implementation of the {@link BufferAccumulator}. The {@link BufferAccumulator} receives the
 * records from {@link TieredStoreProducer} and the records will accumulate and transform to
 * finished {@link * MemorySegment}s. The finished memory segments will be transferred to the
 * corresponding tier dynamically.
 */
public class BufferAccumulatorImpl implements BufferAccumulator {

    private final TierStorage[] tierStorages;

    private final TierWriter[] tierWriters;

    private final TierType[] tierTypes;

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
            TierStorage[] tierStorages,
            int numSubpartitions,
            int bufferSize,
            boolean isBroadcastOnly,
            TieredStoreMemoryManager storeMemoryManager,
            @Nullable BufferCompressor bufferCompressor) {
        this.tierStorages = tierStorages;
        this.storeMemoryManager = storeMemoryManager;
        this.bufferCompressor = bufferCompressor;
        this.isBroadcastOnly = isBroadcastOnly;
        this.tierWriters = new TierWriter[tierStorages.length];
        this.subpartitionSegmentIndexes = new int[numSubpartitions];
        this.subpartitionWriterIndex = new int[numSubpartitions];
        this.bufferRecyclers = new BufferRecycler[tierStorages.length];
        this.tierTypes = new TierType[tierStorages.length];

        for (int i = 0; i < tierStorages.length; i++) {
            tierWriters[i] = tierStorages[i].createPartitionTierWriter();
        }

        for (int i = 0; i < tierStorages.length; i++) {
            tierWriters[i] = tierStorages[i].createPartitionTierWriter();
            tierTypes[i] = tierStorages[i].getTierType();
            TierType tierType = tierTypes[i];
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
            int consumerId,
            Buffer.DataType dataType,
            boolean isEndOfPartition)
            throws IOException {
        cachedBuffer.append(record, consumerId, dataType, isEndOfPartition);
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
    }

    public void release() {
        Arrays.stream(tierStorages).forEach(TierStorage::release);
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
            storeMemoryManager.decRequestedBufferInAccumulator();
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
        if (tierStorages.length == 1) {
            return 0;
        }
        // only for test case Memory and Disk
        if (tierStorages.length == 2
                && tierStorages[0] instanceof MemoryTierStorage
                && tierStorages[1] instanceof DiskTierStorage) {
            if (!isBroadcastOnly && tierStorages[0].canStoreNextSegment(targetSubpartition)) {
                return 0;
            }
            return 1;
        }
        for (int tierIndex = 0; tierIndex < tierStorages.length; ++tierIndex) {
            TierStorage tierStorage = tierStorages[tierIndex];
            if (isBroadcastOnly && tierStorage instanceof MemoryTierStorage) {
                continue;
            }
            if (tierStorages[tierIndex].canStoreNextSegment(targetSubpartition)) {
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
