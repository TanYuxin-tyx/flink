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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.TierType;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.OutputMetrics;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TierStorage;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TierStorageWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoreMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.TieredStoreProducer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.local.disk.DiskTierStorage;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.local.memory.MemoryTierStorage;
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
 * finished {@link MemorySegment}s. The finished memory segments will be transferred to the
 * corresponding tier dynamically.
 */
public class BufferAccumulatorImpl implements BufferAccumulator {

    private final TierStorage[] tierStorages;

    private final TierStorageWriter[] tierStorageWriters;

    private final TierType[] tierTypes;

    private final boolean isBroadcastOnly;

    private final BufferCompressor bufferCompressor;

    private final HashBasedCachedBuffer cachedBuffer;

    private final TieredStoreMemoryManager storeMemoryManager;

    private final BufferRecycler[] bufferRecyclers;

    /** Records the newest segment index belonged to each subpartition. */
    private final int[] subpartitionSegmentIndexes;

    /** Records the newest segment index belonged to each subpartition. */
    private final int[] lastSubpartitionSegmentIndexes;

    /** Record the index of tier writer currently used by each subpartition. */
    private final int[] subpartitionWriterIndex;

    @Nullable private OutputMetrics outputMetrics;

    public BufferAccumulatorImpl(
            TierStorage[] tierStorages,
            int numConsumers,
            int bufferSize,
            boolean isBroadcastOnly,
            TieredStoreMemoryManager storeMemoryManager,
            @Nullable BufferCompressor bufferCompressor) {
        this.tierStorages = tierStorages;
        this.storeMemoryManager = storeMemoryManager;
        this.bufferCompressor = bufferCompressor;
        this.isBroadcastOnly = isBroadcastOnly;
        this.tierStorageWriters = new TierStorageWriter[tierStorages.length];
        this.subpartitionSegmentIndexes = new int[numConsumers];
        this.lastSubpartitionSegmentIndexes = new int[numConsumers];
        Arrays.fill(lastSubpartitionSegmentIndexes, -1);
        this.subpartitionWriterIndex = new int[numConsumers];
        this.bufferRecyclers = new BufferRecycler[tierStorages.length];
        this.tierTypes = new TierType[tierStorages.length];

        for (int i = 0; i < tierStorages.length; i++) {
            tierStorageWriters[i] = tierStorages[i].createTierStorageWriter();
        }

        for (int i = 0; i < tierStorages.length; i++) {
            tierStorageWriters[i] = tierStorages[i].createTierStorageWriter();
            tierTypes[i] = tierStorages[i].getTierType();
            TierType tierType = tierTypes[i];
            bufferRecyclers[i] = buffer -> storeMemoryManager.recycleBuffer(buffer, tierType);
        }

        this.cachedBuffer =
                new HashBasedCachedBuffer(
                        numConsumers, bufferSize, storeMemoryManager, this::writeFinishedBuffer);

        Arrays.fill(subpartitionSegmentIndexes, 0);
        Arrays.fill(subpartitionWriterIndex, -1);
    }

    @Override
    public void receive(ByteBuffer record, int consumerId, Buffer.DataType dataType)
            throws IOException {
        cachedBuffer.append(record, consumerId, dataType);
    }

    @Override
    public void writeFinishedBuffer(List<MemorySegmentAndChannel> memorySegmentAndChannels) {
        try {
            writeBuffers(memorySegmentAndChannels);
        } catch (IOException e) {
            ExceptionUtils.rethrow(e);
        }
    }

    @Override
    public void setMetricGroup(OutputMetrics metrics) {
        this.outputMetrics = checkNotNull(metrics);
    }

    public void close() {
        Arrays.stream(tierStorageWriters).forEach(TierStorageWriter::close);
    }

    public void release() {
        Arrays.stream(tierStorages).forEach(TierStorage::release);
    }

    void writeBuffers(List<MemorySegmentAndChannel> finishedSegments) throws IOException {
        for (MemorySegmentAndChannel finishedSegment : finishedSegments) {
            writeFinishedBuffer(finishedSegment);
        }
    }

    private void writeFinishedBuffer(MemorySegmentAndChannel finishedSegment) throws IOException {
        int consumerId = finishedSegment.getChannelIndex();
        int tierIndex = subpartitionWriterIndex[consumerId];
        // For the first buffer
        if (tierIndex == -1) {
            tierIndex = chooseStorageTierIndex(consumerId);
            subpartitionWriterIndex[consumerId] = tierIndex;
        }

        int segmentIndex = subpartitionSegmentIndexes[consumerId];
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
        if (segmentIndex != lastSubpartitionSegmentIndexes[consumerId]) {
            tierStorageWriters[tierIndex].startSegment(consumerId, segmentIndex);
            lastSubpartitionSegmentIndexes[consumerId] = segmentIndex;
        }
        boolean isLastBufferInSegment =
                tierStorageWriters[tierIndex].write(consumerId, compressedBuffer);
        storeMemoryManager.checkNeedTriggerFlushCachedBuffers();
        if (isLastBufferInSegment) {
            tierIndex = chooseStorageTierIndex(consumerId);
            subpartitionWriterIndex[consumerId] = tierIndex;
            subpartitionSegmentIndexes[consumerId] = (segmentIndex + 1);
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
