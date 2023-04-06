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

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.cache.BufferAccumulator;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.cache.BufferAccumulatorImpl;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.OutputMetrics;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.StorageTier;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreMemoryManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreProducer;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * This is a common entrypoint of the emitted records. These records will be emitted to the {@link
 * BufferAccumulator} to accumulate and transform into finished buffers.
 */
public class TieredStoreProducerImpl implements TieredStoreProducer {

    private final boolean isBroadcastOnly;

    private final int numSubpartitions;

    private final BufferAccumulator bufferAccumulator;

    public TieredStoreProducerImpl(
            StorageTier[] storageTiers,
            int numSubpartitions,
            int bufferSize,
            boolean isBroadcastOnly,
            TieredStoreMemoryManager storeMemoryManager,
            @Nullable BufferCompressor bufferCompressor) {
        this.isBroadcastOnly = isBroadcastOnly;
        this.numSubpartitions = numSubpartitions;

        this.bufferAccumulator =
                new BufferAccumulatorImpl(
                        storageTiers,
                        numSubpartitions,
                        bufferSize,
                        isBroadcastOnly,
                        storeMemoryManager,
                        bufferCompressor);
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
                bufferAccumulator.receive(record.duplicate(), i, dataType, isEndOfPartition);
            }
        } else {
            bufferAccumulator.receive(record, targetSubpartition, dataType, isEndOfPartition);
        }
    }

    @Override
    public void setMetricGroup(OutputMetrics metrics) {
        bufferAccumulator.setMetricGroup(metrics);
    }

    @Override
    public void close() {
        bufferAccumulator.close();
    }

    @Override
    public void release() {
        bufferAccumulator.release();
    }
}
