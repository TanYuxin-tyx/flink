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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.OutputMetrics;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierProducerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.local.disk.DiskTierProducerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.local.memory.MemoryTierProducerAgent;
import org.apache.flink.util.ExceptionUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This is a common entrypoint of the emitted records. These records will be emitted to the {@link
 * BufferAccumulator} to accumulate and transform into finished buffers.
 */
public class TieredStorageProducerClientImpl implements TieredStorageProducerClient {

    private final boolean isBroadcastOnly;

    private final int numConsumers;

    private final BufferAccumulator bufferAccumulator;

    private final BufferCompressor bufferCompressor;

    private final TieredStorageMemoryManager storageMemoryManager;

    private final CacheFlushManager cacheFlushManager;

    private final List<TierProducerAgent> tierProducerAgents;

    private OutputMetrics outputMetrics;

    private final BufferRecycler[] bufferRecyclers;

    /** Records the newest segment index belonged to each subpartition. */
    private final int[] subpartitionSegmentIndexes;

    /** Records the newest segment index belonged to each subpartition. */
    private final int[] lastSubpartitionSegmentIndexes;

    /** Record the index of tier writer currently used by each subpartition. */
    private final int[] subpartitionWriterIndex;

    public TieredStorageProducerClientImpl(
            int numConsumers,
            boolean isBroadcastOnly,
            BufferAccumulator bufferAccumulator,
            @Nullable BufferCompressor bufferCompressor,
            TieredStorageMemoryManager storageMemoryManager,
            CacheFlushManager cacheFlushManager,
            List<TierProducerAgent> tierProducerAgents) {
        this.isBroadcastOnly = isBroadcastOnly;
        this.numConsumers = numConsumers;
        this.bufferAccumulator = bufferAccumulator;
        this.bufferCompressor = bufferCompressor;
        this.storageMemoryManager = storageMemoryManager;
        this.cacheFlushManager = cacheFlushManager;
        this.tierProducerAgents = tierProducerAgents;
        this.subpartitionSegmentIndexes = new int[numConsumers];
        this.lastSubpartitionSegmentIndexes = new int[numConsumers];
        this.subpartitionWriterIndex = new int[numConsumers];
        this.bufferRecyclers = new BufferRecycler[tierProducerAgents.size()];

        for (int i = 0; i < tierProducerAgents.size(); i++) {
            final int tierIndex = i;
            bufferRecyclers[i] = buffer -> storageMemoryManager.recycleBuffer(buffer, tierIndex);
        }
        Arrays.fill(subpartitionSegmentIndexes, 0);
        Arrays.fill(lastSubpartitionSegmentIndexes, -1);
        Arrays.fill(subpartitionWriterIndex, -1);

        bufferAccumulator.setup(numConsumers, this::writeFinishedBuffers);
    }

    @Override
    public void emit(
            ByteBuffer record, int consumerId, Buffer.DataType dataType, boolean isBroadcast)
            throws IOException {

        if (isBroadcast && !isBroadcastOnly) {
            for (int i = 0; i < numConsumers; ++i) {
                bufferAccumulator.receive(record.duplicate(), i, dataType);
            }
        } else {
            bufferAccumulator.receive(record, consumerId, dataType);
        }
    }

    @Override
    public void setMetricGroup(OutputMetrics outputMetrics) {
        this.outputMetrics = outputMetrics;
    }

    @Override
    public void close() {
        bufferAccumulator.close();
    }

    @Override
    public void release() {
        tierProducerAgents.forEach(TierProducerAgent::release);
    }

    public void writeFinishedBuffers(int subpartitionId, List<Buffer> finishedBuffers) {
        try {
            writeBuffers(subpartitionId, finishedBuffers);
        } catch (IOException e) {
            ExceptionUtils.rethrow(e);
        }
    }

    void writeBuffers(int subpartitionId, List<Buffer> finishedBuffers) throws IOException {
        for (Buffer finishedBuffer : finishedBuffers) {
            writeFinishedBuffer(subpartitionId, finishedBuffer);
        }
    }

    private void writeFinishedBuffer(int subpartitionId, Buffer finishedBuffer) throws IOException {
        int tierIndex = subpartitionWriterIndex[subpartitionId];
        // For the first buffer
        if (tierIndex == -1) {
            tierIndex = chooseStorageTierIndex(subpartitionId);
            subpartitionWriterIndex[subpartitionId] = tierIndex;
        }

        int segmentIndex = subpartitionSegmentIndexes[subpartitionId];
        if (finishedBuffer.getDataType().isBuffer()) {
            storageMemoryManager.decNumRequestedBufferInAccumulator();
            storageMemoryManager.incNumRequestedBuffer(tierIndex);
        }
        Buffer networkBuffer =
                new NetworkBuffer(
                        finishedBuffer.getMemorySegment(),
                        finishedBuffer.getDataType().isBuffer()
                                ? bufferRecyclers[tierIndex]
                                : FreeingBufferRecycler.INSTANCE,
                        finishedBuffer.getDataType(),
                        finishedBuffer.getSize());
        Buffer compressedBuffer = compressBufferIfPossible(networkBuffer);
        updateStatistics(compressedBuffer);
        if (segmentIndex != lastSubpartitionSegmentIndexes[subpartitionId]) {
            tierProducerAgents.get(tierIndex).startSegment(subpartitionId, segmentIndex);
            lastSubpartitionSegmentIndexes[subpartitionId] = segmentIndex;
        }
        boolean isLastBufferInSegment =
                tierProducerAgents.get(tierIndex).write(subpartitionId, compressedBuffer);
        cacheFlushManager.checkNeedTriggerFlushCachedBuffers();
        if (isLastBufferInSegment) {
            tierIndex = chooseStorageTierIndex(subpartitionId);
            subpartitionWriterIndex[subpartitionId] = tierIndex;
            subpartitionSegmentIndexes[subpartitionId] = (segmentIndex + 1);
        }
    }

    private int chooseStorageTierIndex(int targetSubpartition) throws IOException {
        if (tierProducerAgents.size() == 1) {
            return 0;
        }
        // only for test case Memory and Disk
        if (tierProducerAgents.size() == 2
                && tierProducerAgents.get(0) instanceof MemoryTierProducerAgent
                && tierProducerAgents.get(1) instanceof DiskTierProducerAgent) {
            if (!isBroadcastOnly
                    && tierProducerAgents.get(0).canStoreNextSegment(targetSubpartition)) {
                return 0;
            }
            return 1;
        }
        for (int tierIndex = 0; tierIndex < tierProducerAgents.size(); ++tierIndex) {
            TierProducerAgent tierProducerAgent = tierProducerAgents.get(tierIndex);
            if (isBroadcastOnly && tierProducerAgent instanceof MemoryTierProducerAgent) {
                continue;
            }
            if (tierProducerAgents.get(tierIndex).canStoreNextSegment(targetSubpartition)) {
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