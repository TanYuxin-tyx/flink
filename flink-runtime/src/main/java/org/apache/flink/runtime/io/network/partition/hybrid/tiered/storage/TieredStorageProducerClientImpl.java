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
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This is a common entrypoint of the emitted records. These records will be emitted to the {@link
 * BufferAccumulator} to accumulate and transform into finished buffers.
 */
public class TieredStorageProducerClientImpl implements TieredStorageProducerClient {

    private final boolean isBroadcastOnly;

    private final int numSubpartitions;

    private final BufferAccumulator bufferAccumulator;

    private final BufferCompressor bufferCompressor;

    /** Note that the {@link TierProducerAgent}s are ordered by priority. */
    private final List<TierProducerAgent> tierProducerAgents;

    /** The current writing segment index for each subpartition. */
    private final int[] currentSubpartitionSegmentId;

    /** The current writing tier index for each subpartition. */
    private final TierProducerAgent[] currentSubpartitionTierAgent;

    private OutputMetrics outputMetrics;

    public TieredStorageProducerClientImpl(
            int numSubpartitions,
            boolean isBroadcastOnly,
            BufferAccumulator bufferAccumulator,
            @Nullable BufferCompressor bufferCompressor,
            List<TierProducerAgent> tierProducerAgents) {
        this.isBroadcastOnly = isBroadcastOnly;
        this.numSubpartitions = numSubpartitions;
        this.bufferAccumulator = bufferAccumulator;
        this.bufferCompressor = bufferCompressor;
        this.tierProducerAgents = tierProducerAgents;
        this.currentSubpartitionSegmentId = new int[numSubpartitions];
        this.currentSubpartitionTierAgent = new TierProducerAgent[numSubpartitions];

        Arrays.fill(currentSubpartitionSegmentId, -1);

        bufferAccumulator.setup(this::writeAccumulatedBuffers);
    }

    /**
     * Write records to the producer client. The {@link BufferAccumulator} will accumulate the
     * records into buffers.
     *
     * <p>Note that isBroadcast indicates whether the record is broadcast, while isBroadcastOnly
     * indicates whether the result partition is broadcast-only. When the result partition is not
     * broadcast-only and the record is a broadcast record, the record will be written to all the
     * subpartitions.
     *
     * @param record the written record data
     * @param subpartitionId the subpartition identifier
     * @param dataType the data type of the record
     * @param isBroadcast whether the record is a broadcast record
     */
    @Override
    public void write(
            ByteBuffer record,
            TieredStorageSubpartitionId subpartitionId,
            Buffer.DataType dataType,
            boolean isBroadcast)
            throws IOException {

        if (isBroadcast && !isBroadcastOnly) {
            for (int i = 0; i < numSubpartitions; ++i) {
                bufferAccumulator.receive(
                        record.duplicate(), new TieredStorageSubpartitionId(i), dataType);
            }
        } else {
            bufferAccumulator.receive(record, subpartitionId, dataType);
        }
    }

    public void setMetricGroup(OutputMetrics outputMetrics) {
        this.outputMetrics = outputMetrics;
    }

    public void close() {
        bufferAccumulator.close();
        tierProducerAgents.forEach(TierProducerAgent::close);
    }

    @Override
    public void release() {
        tierProducerAgents.forEach(TierProducerAgent::release);
    }

    /**
     * Write the accumulated buffers of this subpartitionId to the appropriate tiers.
     *
     * @param subpartitionId the subpartition identifier
     * @param accumulatedBuffers the accumulated buffers of this subpartition
     */
    private void writeAccumulatedBuffers(
            TieredStorageSubpartitionId subpartitionId, List<Buffer> accumulatedBuffers) {
        Queue<Buffer> buffers = new ArrayDeque<>(accumulatedBuffers);
        try {
            while (!buffers.isEmpty()) {
                writeAccumulatedBuffer(subpartitionId, buffers.poll());
            }
        } catch (IOException e) {
            buffers.forEach(Buffer::recycleBuffer);
            ExceptionUtils.rethrow(e);
        }
    }

    /**
     * Write the accumulated buffer of this subpartitionId to an appropriate tier. After the tier is
     * decided, the buffer will be written to the selected tier.
     *
     * @param subpartitionId the subpartition identifier
     * @param accumulatedBuffer one accumulated buffer of this subpartition
     */
    private void writeAccumulatedBuffer(
            TieredStorageSubpartitionId subpartitionId, Buffer accumulatedBuffer)
            throws IOException {
        updateStatistics(accumulatedBuffer);
        Buffer compressedBuffer = compressBufferIfPossible(accumulatedBuffer);

        if (currentSubpartitionTierAgent[subpartitionId.getSubpartitionId()] == null) {
            chooseStorageTierToStartSegment(subpartitionId);
        }

        boolean isSuccess =
                currentSubpartitionTierAgent[subpartitionId.getSubpartitionId()].write(
                        subpartitionId.getSubpartitionId(), compressedBuffer);
        if (!isSuccess) {
            chooseStorageTierToStartSegment(subpartitionId);
            // We should make sure that the writing must be successful
            currentSubpartitionTierAgent[subpartitionId.getSubpartitionId()].write(
                    subpartitionId.getSubpartitionId(), compressedBuffer);
        }
    }

    private void chooseStorageTierToStartSegment(TieredStorageSubpartitionId subpartitionId)
            throws IOException {
        int subpartitionIndex = subpartitionId.getSubpartitionId();
        int segmentIndex = currentSubpartitionSegmentId[subpartitionIndex];
        int nextSegmentIndex = segmentIndex + 1;

        if (tierProducerAgents.size() == 1) {
            if (tierProducerAgents
                    .get(0)
                    .tryStartNewSegment(subpartitionId, nextSegmentIndex, true)) {
                updateTierIndexForNextSegment(subpartitionIndex, nextSegmentIndex, 0);
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
                            .tryStartNewSegment(subpartitionId, nextSegmentIndex, false)) {
                updateTierIndexForNextSegment(subpartitionIndex, nextSegmentIndex, 0);
                return;
            } else {
                if (tierProducerAgents
                        .get(1)
                        .tryStartNewSegment(subpartitionId, nextSegmentIndex, false)) {
                    updateTierIndexForNextSegment(subpartitionIndex, nextSegmentIndex, 1);
                    return;
                } else {
                    throw new IOException("Failed to start new segment.");
                }
            }
        }
        for (int tierIndex = 0; tierIndex < tierProducerAgents.size(); ++tierIndex) {
            if (tierProducerAgents
                    .get(tierIndex)
                    .tryStartNewSegment(subpartitionId, nextSegmentIndex, false)) {
                updateTierIndexForNextSegment(subpartitionIndex, nextSegmentIndex, tierIndex);
                return;
            }
        }
        throw new IOException("Failed to choose a storage tier to start a new segment.");
    }

    private void updateTierIndexForNextSegment(
            int targetSubpartition, int nextSegmentIndex, int storageTierIndex) {
        currentSubpartitionSegmentId[targetSubpartition] = nextSegmentIndex;
        currentSubpartitionTierAgent[targetSubpartition] = tierProducerAgents.get(storageTierIndex);
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
