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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.tier.local.memory;

import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.TierStorage;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.TierStorageFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.TieredStoreMemoryManager;

public class UpstreamMemoryTierStorageFactory implements TierStorageFactory {

    private final int numSubpartitions;

    private final int bufferSize;

    private final TieredStoreMemoryManager storeMemoryManager;

    private final boolean isBroadcast;

    private final BufferCompressor bufferCompressor;

    public UpstreamMemoryTierStorageFactory(
            int numSubpartitions,
            int bufferSize,
            TieredStoreMemoryManager storeMemoryManager,
            boolean isBroadcast,
            BufferCompressor bufferCompressor) {
        this.numSubpartitions = numSubpartitions;
        this.bufferSize = bufferSize;
        this.storeMemoryManager = storeMemoryManager;
        this.isBroadcast = isBroadcast;
        this.bufferCompressor = bufferCompressor;
    }

    @Override
    public TierStorage createTierStorage() {
        return new MemoryTierStorage(
                numSubpartitions, bufferSize, storeMemoryManager, isBroadcast, bufferCompressor);
    }
}
