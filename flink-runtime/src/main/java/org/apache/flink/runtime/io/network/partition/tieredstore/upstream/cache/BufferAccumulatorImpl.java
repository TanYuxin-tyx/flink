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

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreMemoryManager;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

public class BufferAccumulatorImpl implements BufferAccumulator {

    private final HashBasedCachedBuffer cachedBuffer;

    public BufferAccumulatorImpl(
            int numSubpartitions,
            int bufferSize,
            TieredStoreMemoryManager storeMemoryManager,
            Consumer<CachedBufferContext> finishedBufferListener) {
        this.cachedBuffer =
                new HashBasedCachedBuffer(
                        numSubpartitions, bufferSize, storeMemoryManager, finishedBufferListener);
    }

    @Override
    public void emit(
            ByteBuffer record,
            int targetSubpartition,
            Buffer.DataType dataType,
            boolean isBroadcast,
            boolean isEndOfPartition)
            throws IOException {
        cachedBuffer.append(record, targetSubpartition, dataType, isBroadcast, isEndOfPartition);
    }

    @Override
    public void release() {}
}
