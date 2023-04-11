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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered;

import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.CacheFlushManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.TierStorage;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.TieredStoreMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.UpstreamTieredStoreMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.file.PartitionFileManager;
import org.apache.flink.util.ExceptionUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public abstract class BaseTieredStorageFactory implements TieredStorageFactory {
    private final TierType[] tierTypes;

    protected final ResultPartitionID resultPartitionID;

    protected final int numSubpartitions;

    protected final int bufferSize;

    protected final boolean isBroadcast;

    protected final BufferCompressor bufferCompressor;

    public final Map<TierType, Integer> tierExclusiveBuffers;

    protected final CacheFlushManager cacheFlushManager;

    protected final PartitionFileManager partitionFileManager;

    protected final TieredStoreMemoryManager storeMemoryManager;

    protected final TierStorage[] tierStorages;

    public BaseTieredStorageFactory(
            TierType[] tierTypes,
            ResultPartitionID resultPartitionID,
            int numSubpartitions,
            int bufferSize,
            boolean isBroadcast,
            @Nullable BufferCompressor bufferCompressor,
            PartitionFileManager partitionFileManager,
            UpstreamTieredStoreMemoryManager storeMemoryManager)
            throws IOException {
        this.tierTypes = tierTypes;
        this.resultPartitionID = resultPartitionID;
        this.numSubpartitions = numSubpartitions;
        this.bufferSize = bufferSize;
        this.isBroadcast = isBroadcast;
        this.bufferCompressor = bufferCompressor;
        this.tierStorages = new TierStorage[tierTypes.length];
        this.tierExclusiveBuffers = new HashMap<>();
        this.cacheFlushManager = new CacheFlushManager();
        this.partitionFileManager = partitionFileManager;
        this.storeMemoryManager = storeMemoryManager;
    }

    public void setup() {
        try {
            for (int i = 0; i < tierTypes.length; i++) {
                tierStorages[i] = createTierStorage(tierTypes[i]);
            }
        } catch (IOException e) {
            ExceptionUtils.rethrow(e);
        }
    }

    @Override
    public TierStorage[] getTierStorages() {
        return tierStorages;
    }

    abstract TierStorage createTierStorage(TierType tierType) throws IOException;
}
