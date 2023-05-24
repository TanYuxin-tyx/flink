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
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.OutputMetrics;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
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

    private final List<TierProducerAgent> tierProducerAgents;

    private OutputMetrics outputMetrics;

    /** Records the newest segment index belonged to each subpartition. */
    private final int[] currentSubpartitionSegmentId;

    /** Record the index of tier writer currently used by each subpartition. */
    private final int[] currentSubpartitionTierIndex;

    private final boolean[] isSubpartitionSegmentFinished;

    public TieredStorageProducerClientImpl(
            int numConsumers,
            boolean isBroadcastOnly,
            BufferAccumulator bufferAccumulator,
            @Nullable BufferCompressor bufferCompressor,
            List<TierProducerAgent> tierProducerAgents) {
        this.isBroadcastOnly = isBroadcastOnly;
        this.numConsumers = numConsumers;
        this.bufferAccumulator = bufferAccumulator;
        this.bufferCompressor = bufferCompressor;
        this.tierProducerAgents = tierProducerAgents;
        this.currentSubpartitionSegmentId = new int[numConsumers];
        this.currentSubpartitionTierIndex = new int[numConsumers];
        this.isSubpartitionSegmentFinished = new boolean[numConsumers];

        Arrays.fill(currentSubpartitionSegmentId, -1);
        Arrays.fill(currentSubpartitionTierIndex, -1);
        Arrays.fill(isSubpartitionSegmentFinished, true);

        bufferAccumulator.setup(numConsumers, this::writeFinishedBuffers);
    }

    @Override
    public void emit(
            ByteBuffer record, int subpartitionId, Buffer.DataType dataType, boolean isBroadcast)
            throws IOException {

        if (isBroadcast && !isBroadcastOnly) {
            for (int i = 0; i < numConsumers; ++i) {
                bufferAccumulator.receive(
                        record.duplicate(), new TieredStorageSubpartitionId(i), dataType);
            }
        } else {
            bufferAccumulator.receive(
                    record, new TieredStorageSubpartitionId(subpartitionId), dataType);
        }
    }

    @Override
    public void setMetricGroup(OutputMetrics outputMetrics) {
        this.outputMetrics = outputMetrics;
    }

    @Override
    public void close() {
        bufferAccumulator.close();
        tierProducerAgents.forEach(TierProducerAgent::close);
    }

    @Override
    public void release() {
        tierProducerAgents.forEach(TierProducerAgent::release);
    }

    public void writeFinishedBuffers(
            TieredStorageSubpartitionId subpartitionId, List<Buffer> finishedBuffers) {
        try {
            writeBuffers(subpartitionId, finishedBuffers);
        } catch (IOException e) {
            ExceptionUtils.rethrow(e);
        }
    }

    void writeBuffers(TieredStorageSubpartitionId subpartitionId, List<Buffer> finishedBuffers)
            throws IOException {
        for (Buffer finishedBuffer : finishedBuffers) {
            writeFinishedBuffer(subpartitionId, finishedBuffer);
        }
    }

    private void writeFinishedBuffer(
            TieredStorageSubpartitionId subpartitionId, Buffer finishedBuffer) throws IOException {
        int subpartitionIndex = subpartitionId.getSubpartitionId();
        int tierIndex = chooseStorageTierIndexIfNeeded(subpartitionIndex);

        Buffer compressedBuffer = compressBufferIfPossible(finishedBuffer);
        updateStatistics(compressedBuffer);
        boolean isLastBufferInSegment =
                tierProducerAgents.get(tierIndex).write(subpartitionIndex, compressedBuffer);
        if (isLastBufferInSegment) {
            isSubpartitionSegmentFinished[subpartitionIndex] = true;
        }
    }

    private int chooseStorageTierIndexIfNeeded(int subpartitionIndex) throws IOException {
        if (isSubpartitionSegmentFinished[subpartitionIndex]) {
            chooseStorageTierIndex(subpartitionIndex);
            isSubpartitionSegmentFinished[subpartitionIndex] = false;
        }
        return currentSubpartitionTierIndex[subpartitionIndex];
    }

    private void chooseStorageTierIndex(int targetSubpartition) throws IOException {
        int segmentIndex = currentSubpartitionSegmentId[targetSubpartition];
        TieredStorageSubpartitionId storageSubpartitionId =
                new TieredStorageSubpartitionId(targetSubpartition);
        int nextSegmentIndex = segmentIndex + 1;
        if (tierProducerAgents.size() == 1) {
            if (tierProducerAgents
                    .get(0)
                    .tryStartNewSegment(storageSubpartitionId, nextSegmentIndex, true)) {
                updateTierIndexForNextSegment(targetSubpartition, nextSegmentIndex, 0);
                return;
            }
        }
        // only for test case Memory and Disk
        if (tierProducerAgents.size() == 2
                && tierProducerAgents.get(0) instanceof MemoryTierProducerAgent
                && tierProducerAgents.get(1) instanceof DiskTierProducerAgent) {
            if (!isBroadcastOnly
                    && tierProducerAgents
                            .get(0)
                            .tryStartNewSegment(storageSubpartitionId, nextSegmentIndex, false)) {
                updateTierIndexForNextSegment(targetSubpartition, nextSegmentIndex, 0);
                return;
            } else {
                if (tierProducerAgents
                        .get(1)
                        .tryStartNewSegment(storageSubpartitionId, nextSegmentIndex, false)) {
                    updateTierIndexForNextSegment(targetSubpartition, nextSegmentIndex, 1);
                    return;
                } else {
                    throw new IOException("Failed to start new segment.");
                }
            }
        }
        for (int tierIndex = 0; tierIndex < tierProducerAgents.size(); ++tierIndex) {
            if (tierProducerAgents
                    .get(tierIndex)
                    .tryStartNewSegment(storageSubpartitionId, nextSegmentIndex, false)) {
                updateTierIndexForNextSegment(targetSubpartition, nextSegmentIndex, tierIndex);
                return;
            }
        }
        throw new IOException("All gates are full, cannot select the writer of gate");
    }

    private void updateTierIndexForNextSegment(
            int targetSubpartition, int nextSegmentIndex, int storageTierIndex) {
        currentSubpartitionSegmentId[targetSubpartition] = nextSegmentIndex;
        currentSubpartitionTierIndex[targetSubpartition] = storageTierIndex;
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
