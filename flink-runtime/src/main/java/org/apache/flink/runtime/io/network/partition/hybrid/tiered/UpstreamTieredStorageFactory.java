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
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.TierStorageFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.UpstreamTieredStoreMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.file.PartitionFileManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.tier.local.disk.UpstreamDiskTierStorageFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.tier.local.memory.UpstreamMemoryTierStorageFactory;

import javax.annotation.Nullable;

import java.io.IOException;

public class UpstreamTieredStorageFactory extends BaseTieredStorageFactory {

    private final float minReservedDiskSpaceFraction;

    private final String dataFileBasePath;

    public UpstreamTieredStorageFactory(
            TierType[] tierTypes,
            ResultPartitionID resultPartitionID,
            int numSubpartitions,
            int bufferSize,
            float minReservedDiskSpaceFraction,
            String dataFileBasePath,
            boolean isBroadcast,
            @Nullable BufferCompressor bufferCompressor,
            PartitionFileManager partitionFileManager,
            UpstreamTieredStoreMemoryManager storeMemoryManager,
            CacheFlushManager cacheFlushManager)
            throws IOException {
        super(
                tierTypes,
                resultPartitionID,
                numSubpartitions,
                bufferSize,
                isBroadcast,
                bufferCompressor,
                partitionFileManager,
                storeMemoryManager,
                cacheFlushManager);
        this.minReservedDiskSpaceFraction = minReservedDiskSpaceFraction;
        this.dataFileBasePath = dataFileBasePath;
    }

    @Override
    public TierStorage createTierStorage(TierType tierType) throws IOException {
        TierStorageFactory tierStorageFactory;
        switch (tierType) {
            case IN_MEM:
                tierStorageFactory = getMemoryTierStorageFactory();
                break;
            case IN_DISK:
                tierStorageFactory = getDiskTierStorageFactory();
                break;
            default:
                throw new IllegalArgumentException("Illegal tier type " + tierType);
        }
        TierStorage tierStorage = tierStorageFactory.createTierStorage();
        tierStorage.setup();
        return tierStorage;
    }

    private TierStorageFactory getMemoryTierStorageFactory() {
        return new UpstreamMemoryTierStorageFactory(
                numSubpartitions, bufferSize, storeMemoryManager, isBroadcast, bufferCompressor);
    }

    private TierStorageFactory getDiskTierStorageFactory() {
        return new UpstreamDiskTierStorageFactory(
                numSubpartitions,
                bufferSize,
                resultPartitionID,
                storeMemoryManager,
                cacheFlushManager,
                dataFileBasePath,
                minReservedDiskSpaceFraction,
                isBroadcast,
                bufferCompressor,
                partitionFileManager);
    }
}
