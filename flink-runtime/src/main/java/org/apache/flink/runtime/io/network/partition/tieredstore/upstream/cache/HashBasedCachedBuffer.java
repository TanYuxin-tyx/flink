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

package org.apache.flink.runtime.io.network.partition.tieredstore.upstream.cache;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.TieredStoreMode;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreMemoryManager;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

public class HashBasedCachedBuffer implements CacheBufferOperation {
    private final SubpartitionCachedBuffer[] subpartitionCachedBuffers;

    private final TieredStoreMemoryManager storeMemoryManager;

    HashBasedCachedBuffer(
            int numSubpartitions,
            int bufferSize,
            BufferCompressor bufferCompressor,
            TieredStoreMemoryManager storeMemoryManager,
            Consumer<CachedBufferContext> finishedBufferListener) {
        this.subpartitionCachedBuffers = new SubpartitionCachedBuffer[numSubpartitions];
        this.storeMemoryManager = storeMemoryManager;

        for (int i = 0; i < numSubpartitions; i++) {
            subpartitionCachedBuffers[i] =
                    new SubpartitionCachedBuffer(
                            i, bufferSize, bufferCompressor, finishedBufferListener, this);
        }
    }

    public void append(
            ByteBuffer record,
            int targetChannel,
            Buffer.DataType dataType,
            boolean isBroadcast,
            boolean isEndOfPartition)
            throws IOException {
        try {
            getSubpartitionCachedBuffer(targetChannel)
                    .append(record, dataType, isBroadcast, isEndOfPartition);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    @Override
    public BufferBuilder requestBufferFromPool() throws InterruptedException {
        MemorySegment segment =
                storeMemoryManager.requestMemorySegmentBlocking(
                        TieredStoreMode.TieredType.IN_CACHE);
        return new BufferBuilder(segment, this::recycleBuffer);
    }

    private void recycleBuffer(MemorySegment buffer) {
        storeMemoryManager.recycleBuffer(buffer, TieredStoreMode.TieredType.IN_CACHE);
    }

    private SubpartitionCachedBuffer getSubpartitionCachedBuffer(int targetChannel) {
        return subpartitionCachedBuffers[targetChannel];
    }
}
