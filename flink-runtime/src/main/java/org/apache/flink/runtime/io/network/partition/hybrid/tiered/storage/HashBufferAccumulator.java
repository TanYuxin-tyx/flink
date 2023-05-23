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
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;

/**
 * The hash implementation of the {@link BufferAccumulator}. The {@link BufferAccumulator} receives
 * the records from {@link TieredStorageProducerClient} and the records will accumulate and
 * transform to finished buffers. The finished buffers will be transferred to the corresponding tier
 * dynamically.
 */
public class HashBufferAccumulator implements BufferAccumulator, HashBufferAccumulatorOperation {

    private final int bufferSize;

    private final TieredStorageMemoryManager storageMemoryManager;

    private SubpartitionHashBufferAccumulator[] subpartitionHashBufferAccumulators;

    public HashBufferAccumulator(int bufferSize, TieredStorageMemoryManager storageMemoryManager) {
        this.bufferSize = bufferSize;
        this.storageMemoryManager = storageMemoryManager;
    }

    @Override
    public void setup(
            int numSubpartitions,
            BiConsumer<TieredStorageSubpartitionId, List<Buffer>> accumulatedBufferFlusher) {
        this.subpartitionHashBufferAccumulators =
                new SubpartitionHashBufferAccumulator[numSubpartitions];

        for (int i = 0; i < numSubpartitions; i++) {
            subpartitionHashBufferAccumulators[i] =
                    new SubpartitionHashBufferAccumulator(
                            new TieredStorageSubpartitionId(i), bufferSize, this);
        }

        for (int i = 0; i < numSubpartitions; i++) {
            subpartitionHashBufferAccumulators[i].setup(accumulatedBufferFlusher);
        }
    }

    @Override
    public void receive(
            ByteBuffer record, TieredStorageSubpartitionId subpartitionId, Buffer.DataType dataType)
            throws IOException {
        try {
            getSubpartitionHashAccumulator(subpartitionId).append(record, dataType);
        } catch (InterruptedException e) {
            ExceptionUtils.rethrow(e);
        }
    }

    @Override
    public void close() {
        Arrays.stream(subpartitionHashBufferAccumulators)
                .forEach(SubpartitionHashBufferAccumulator::close);
    }

    @Override
    public BufferBuilder requestBufferBlocking() {
        return storageMemoryManager.requestBufferBlocking(this);
    }

    private SubpartitionHashBufferAccumulator getSubpartitionHashAccumulator(
            TieredStorageSubpartitionId subpartitionId) {
        return subpartitionHashBufferAccumulators[subpartitionId.getSubpartitionId()];
    }
}
