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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.shuffle.TierType;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.OutputMetrics;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The implementation of the {@link BufferAccumulator}. The {@link BufferAccumulator} receives the
 * records from {@link TieredStorageProducerClient} and the records will accumulate and transform to
 * finished {@link MemorySegment}s. The finished memory segments will be transferred to the
 * corresponding tier dynamically.
 */
public class BufferAccumulatorImpl implements BufferAccumulator {

    private final List<TierProducerAgent> tierProducerAgents;

    private final TierType[] tierTypes;

    private final boolean isBroadcastOnly;

    private final BufferCompressor bufferCompressor;

    private final HashBasedCachedBuffer cachedBuffer;

    private final TieredStorageMemoryManager storageMemoryManager;

    /** Records the newest segment index belonged to each subpartition. */
    private final int[] subpartitionSegmentIndexes;

    /** Records the newest segment index belonged to each subpartition. */
    private final int[] lastSubpartitionSegmentIndexes;

    /** Record the index of tier writer currently used by each subpartition. */
    private final int[] subpartitionWriterIndex;

    private int numSubpartitions;
    @Nullable private OutputMetrics outputMetrics;

    public BufferAccumulatorImpl(
            List<TierProducerAgent> tierProducerAgents,
            int numConsumers,
            int bufferSize,
            boolean isBroadcastOnly,
            TieredStorageMemoryManager storageMemoryManager,
            CacheFlushManager cacheFlushManager,
            @Nullable BufferCompressor bufferCompressor) {
        this.tierProducerAgents = tierProducerAgents;
        this.storageMemoryManager = storageMemoryManager;
        this.bufferCompressor = bufferCompressor;
        this.isBroadcastOnly = isBroadcastOnly;
        this.subpartitionSegmentIndexes = new int[numConsumers];
        this.lastSubpartitionSegmentIndexes = new int[numConsumers];
        Arrays.fill(lastSubpartitionSegmentIndexes, -1);
        this.subpartitionWriterIndex = new int[numConsumers];
        this.tierTypes = new TierType[tierProducerAgents.size()];

        for (int i = 0; i < tierProducerAgents.size(); i++) {
            tierTypes[i] = tierProducerAgents.get(i).getTierType();
            TierType tierType = tierTypes[i];
        }

        this.cachedBuffer =
                new HashBasedCachedBuffer(
                        numConsumers, bufferSize, storageMemoryManager, cacheFlushManager);

        Arrays.fill(subpartitionSegmentIndexes, 0);
        Arrays.fill(subpartitionWriterIndex, -1);
    }

    @Override
    public void setup(int numSubpartitions, BiConsumer<Integer, List<Buffer>> bufferFlusher) {
        this.numSubpartitions = numSubpartitions;
        cachedBuffer.setup(bufferFlusher);
    }

    @Override
    public void receive(ByteBuffer record, int consumerId, Buffer.DataType dataType)
            throws IOException {
        cachedBuffer.append(record, consumerId, dataType);
    }

    @Override
    public void setMetricGroup(OutputMetrics metrics) {
        this.outputMetrics = checkNotNull(metrics);
    }

    public void close() {
        tierProducerAgents.forEach(TierProducerAgent::close);
    }

    public void release() {
        tierProducerAgents.forEach(TierProducerAgent::release);
    }
}
