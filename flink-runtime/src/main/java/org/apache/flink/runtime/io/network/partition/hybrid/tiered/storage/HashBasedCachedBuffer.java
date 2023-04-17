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
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.common.TieredStoreMemoryManager;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Consumer;

public class HashBasedCachedBuffer implements CacheBufferOperation {
    private final SubpartitionCachedBuffer[] subpartitionCachedBuffers;

    private final TieredStoreMemoryManager storeMemoryManager;

    HashBasedCachedBuffer(
            int numConsumers,
            int bufferSize,
            TieredStoreMemoryManager storeMemoryManager,
            Consumer<List<MemorySegmentAndConsumerId>> finishedBufferListener) {
        this.subpartitionCachedBuffers = new SubpartitionCachedBuffer[numConsumers];
        this.storeMemoryManager = storeMemoryManager;

        for (int i = 0; i < numConsumers; i++) {
            subpartitionCachedBuffers[i] =
                    new SubpartitionCachedBuffer(i, bufferSize, finishedBufferListener, this);
        }
    }

    public void append(ByteBuffer record, int consumerId, Buffer.DataType dataType)
            throws IOException {
        try {
            getCachedBuffer(consumerId).append(record, dataType);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    @Override
    public BufferBuilder requestBufferFromPool() throws InterruptedException {
        MemorySegment segment = storeMemoryManager.requestMemorySegmentInAccumulatorBlocking();
        return new BufferBuilder(segment, this::recycleBuffer);
    }

    private void recycleBuffer(MemorySegment buffer) {
        storeMemoryManager.recycleBufferInAccumulator(buffer);
    }

    private SubpartitionCachedBuffer getCachedBuffer(int consumerId) {
        return subpartitionCachedBuffers[consumerId];
    }
}
