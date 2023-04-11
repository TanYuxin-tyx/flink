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

import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.CacheFlushManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.TierStorage;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.TierStorageFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.TieredStoreMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.UpstreamTieredStoreMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.file.PartitionFileManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.tier.local.disk.UpstreamDiskTierStorageFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.tier.local.memory.UpstreamMemoryTierStorageFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.tier.remote.RemoteTierStorage;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class UpstreamTieredStorageFactory implements TieredStorageFactory {

    private final TierType[] tierTypes;

    private final ResultPartitionID resultPartitionID;

    private final int numSubpartitions;

    private final int bufferSize;

    private final boolean isBroadcast;

    private final BufferCompressor bufferCompressor;

    private final float minReservedDiskSpaceFraction;

    private final String dataFileBasePath;

    private final String baseRemoteStoragePath;

    private final TierStorage[] tierStorages;

    public final Map<TierType, Integer> tierExclusiveBuffers;

    private final CacheFlushManager cacheFlushManager;

    private final PartitionFileManager partitionFileManager;

    private final TieredStoreMemoryManager storeMemoryManager;

    public UpstreamTieredStorageFactory(
            TierType[] tierTypes,
            ResultPartitionID resultPartitionID,
            int numSubpartitions,
            int bufferSize,
            float minReservedDiskSpaceFraction,
            String dataFileBasePath,
            String baseRemoteStoragePath,
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
        this.minReservedDiskSpaceFraction = minReservedDiskSpaceFraction;
        this.dataFileBasePath = dataFileBasePath;
        this.baseRemoteStoragePath = baseRemoteStoragePath;
        this.tierStorages = new TierStorage[tierTypes.length];
        this.tierExclusiveBuffers = new HashMap<>();
        this.cacheFlushManager = new CacheFlushManager();
        this.partitionFileManager = partitionFileManager;
        this.storeMemoryManager = storeMemoryManager;

        setup();
    }

    @Override
    public TierStorage[] getTierStorages() {
        return tierStorages;
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

    private TierStorage createTierStorage(TierType tierType) throws IOException {
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

    private RemoteTierStorage getRemoteTierStorage() {
        if (StringUtils.isNullOrWhitespaceOnly(baseRemoteStoragePath)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Must specify DFS home path by %s when using DFS in Tiered Store.",
                            NettyShuffleEnvironmentOptions
                                    .NETWORK_HYBRID_SHUFFLE_REMOTE_STORAGE_BASE_HOME_PATH
                                    .key()));
        }
        return new RemoteTierStorage(
                numSubpartitions,
                bufferSize,
                storeMemoryManager,
                cacheFlushManager,
                isBroadcast,
                bufferCompressor,
                partitionFileManager);
    }
}
