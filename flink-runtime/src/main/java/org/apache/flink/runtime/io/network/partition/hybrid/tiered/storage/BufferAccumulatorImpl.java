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
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.BiConsumer;

/**
 * The implementation of the {@link BufferAccumulator}. The {@link BufferAccumulator} receives the
 * records from {@link TieredStorageProducerClient} and the records will accumulate and transform to
 * finished {@link MemorySegment}s. The finished memory segments will be transferred to the
 * corresponding tier dynamically.
 */
public class BufferAccumulatorImpl implements BufferAccumulator {

    private final int bufferSize;

    private final TieredStorageMemoryManager1 storageMemoryManager1;

    private HashBasedCachedBuffer cachedBuffer;

    public BufferAccumulatorImpl(int bufferSize, TieredStorageMemoryManager1 storeMemoryManager1) {
        this.bufferSize = bufferSize;
        this.storageMemoryManager1 = storeMemoryManager1;
    }

    @Override
    public void setup(
            int numSubpartitions,
            BiConsumer<TieredStorageSubpartitionId, List<Buffer>> bufferFlusher) {
        cachedBuffer =
                new HashBasedCachedBuffer(numSubpartitions, bufferSize, storageMemoryManager1);
        cachedBuffer.setup(bufferFlusher);
    }

    @Override
    public void receive(
            ByteBuffer record, TieredStorageSubpartitionId subpartitionId, Buffer.DataType dataType)
            throws IOException {
        cachedBuffer.append(record, subpartitionId, dataType);
    }

    @Override
    public void close() {
        cachedBuffer.close();
    }
}
