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
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.BiConsumer;

public class SortBufferAccumulator implements BufferAccumulator {

    private final TieredStoragePartitionId partitionId;

    private final TieredStorageMemoryManager memoryManager;

    /**
     * The {@link HashBufferAccumulator}'s accumulated buffer flusher is not prepared during
     * construction, requiring the field to be initialized during setup. Therefore, it is necessary
     * to verify whether this field is null before using it.
     */
    @Nullable
    private BiConsumer<TieredStorageSubpartitionId, List<Buffer>> accumulatedBufferFlusher;

    public SortBufferAccumulator(
            TieredStoragePartitionId partitionId,
            TieredStorageResourceRegistry resourceRegistry,
            int numSubpartitions,
            int bufferSize,
            TieredStorageMemoryManager memoryManager) {
        this.partitionId = partitionId;
        this.memoryManager = memoryManager;
    }

    @Override
    public void setup(BiConsumer<TieredStorageSubpartitionId, List<Buffer>> bufferFlusher) {}

    @Override
    public void receive(
            ByteBuffer record, TieredStorageSubpartitionId subpartitionId, Buffer.DataType dataType)
            throws IOException {}

    @Override
    public void close() {}
}
