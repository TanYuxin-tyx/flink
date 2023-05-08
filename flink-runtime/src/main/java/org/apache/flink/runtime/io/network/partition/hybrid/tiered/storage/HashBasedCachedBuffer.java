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
 * The hash-based mode to accumulate the records. Each subpartition use a {@link
 * SubpartitionCachedBuffer} to accumulate the received records.
 *
 * <p>Note that {@link #setup} need an argument of buffer flush listener to accept the finished
 * accumulated buffers.
 */
public class HashBasedCachedBuffer implements HashBasedCacheBufferOperation {

    private final int numSubpartitions;

    private final TieredStorageMemoryManager storageMemoryManager;

    private final SubpartitionCachedBuffer[] subpartitionCachedBuffers;

    HashBasedCachedBuffer(
            int numSubpartitions, int bufferSize, TieredStorageMemoryManager storageMemoryManager) {
        this.numSubpartitions = numSubpartitions;
        this.storageMemoryManager = storageMemoryManager;
        this.subpartitionCachedBuffers = new SubpartitionCachedBuffer[numSubpartitions];

        for (int i = 0; i < numSubpartitions; i++) {
            subpartitionCachedBuffers[i] =
                    new SubpartitionCachedBuffer(
                            new TieredStorageSubpartitionId(i), bufferSize, this);
        }
    }

    public void setup(BiConsumer<TieredStorageSubpartitionId, List<Buffer>> bufferFlusher) {
        for (int i = 0; i < numSubpartitions; i++) {
            subpartitionCachedBuffers[i].setup(bufferFlusher);
        }
    }

    public void append(
            ByteBuffer record, TieredStorageSubpartitionId subpartitionId, Buffer.DataType dataType)
            throws IOException {
        try {
            getCachedBuffer(subpartitionId).append(record, dataType);
        } catch (InterruptedException e) {
            ExceptionUtils.rethrow(e);
        }
    }

    @Override
    public BufferBuilder requestBufferFromPool() {
        return storageMemoryManager.requestBufferBlocking();
    }

    public void close() {
        Arrays.stream(subpartitionCachedBuffers).forEach(SubpartitionCachedBuffer::close);
    }

    private SubpartitionCachedBuffer getCachedBuffer(TieredStorageSubpartitionId subpartitionId) {
        return subpartitionCachedBuffers[subpartitionId.getSubpartitionId()];
    }
}
