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

package org.apache.flink.runtime.io.network.partition.tieredstore.upstream;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.tieredstore.TierType;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.CacheFlushManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.StorageTier;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreMemoryManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.UpstreamTieredStoreMemoryManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.file.PartitionFileManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.file.PartitionFileManagerImpl;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.DiskTier;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.RegionBufferIndexTrackerImpl;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.memory.MemoryTier;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.remote.RemoteTier;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreUtils.DATA_FILE_SUFFIX;
import static org.apache.flink.runtime.shuffle.NettyShuffleUtils.HYBRID_SHUFFLE_TIER_EXCLUSIVE_BUFFERS;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class StorageTierWriterFactory {

    private final TierType[] tierTypes;

    private final ResultPartitionID resultPartitionID;

    private final int numSubpartitions;

    private final int bufferSize;

    private final boolean isBroadcast;

    private final BufferCompressor bufferCompressor;

    private final float numBuffersTriggerFlushRatio;

    private final float minReservedDiskSpaceFraction;

    private final String dataFileBasePath;

    private final String baseRemoteStoragePath;

    private final StorageTier[] storageTiers;

    public final Map<TierType, Integer> tierExclusiveBuffers;

    private final CacheFlushManager cacheFlushManager;

    private final PartitionFileManager partitionFileManager;

    private TieredStoreMemoryManager tieredStoreMemoryManager;

    public StorageTierWriterFactory(
            JobID jobID,
            TierType[] tierTypes,
            ResultPartitionID resultPartitionID,
            int numSubpartitions,
            int bufferSize,
            float numBuffersTriggerFlushRatio,
            float minReservedDiskSpaceFraction,
            String dataFileBasePath,
            String baseRemoteStoragePath,
            boolean isBroadcast,
            BatchShuffleReadBufferPool readBufferPool,
            ScheduledExecutorService readIOExecutor,
            @Nullable BufferCompressor bufferCompressor,
            TieredStoreConfiguration storeConfiguration)
            throws IOException {
        this.tierTypes = tierTypes;
        this.resultPartitionID = resultPartitionID;
        this.numSubpartitions = numSubpartitions;
        this.bufferSize = bufferSize;
        this.isBroadcast = isBroadcast;
        this.bufferCompressor = bufferCompressor;
        this.numBuffersTriggerFlushRatio = numBuffersTriggerFlushRatio;
        this.minReservedDiskSpaceFraction = minReservedDiskSpaceFraction;
        this.dataFileBasePath = dataFileBasePath;
        this.baseRemoteStoragePath = baseRemoteStoragePath;
        this.storageTiers = new StorageTier[tierTypes.length];
        this.tierExclusiveBuffers = new HashMap<>();
        this.cacheFlushManager = new CacheFlushManager();
        this.partitionFileManager =
                new PartitionFileManagerImpl(
                        Paths.get(dataFileBasePath + DATA_FILE_SUFFIX),
                        new RegionBufferIndexTrackerImpl(isBroadcast ? 1 : numSubpartitions),
                        readBufferPool,
                        readIOExecutor,
                        storeConfiguration,
                        numSubpartitions,
                        jobID,
                        resultPartitionID,
                        storeConfiguration.getBaseDfsHomePath());

        setupTierDataWriters();
    }

    public StorageTier[] getStorageTierWriters() {
        return storageTiers;
    }

    public TieredStoreMemoryManager getTieredStoreMemoryManager() {
        return tieredStoreMemoryManager;
    }

    private void setupTierDataWriters() throws IOException {
        addTierExclusiveBuffers(tierTypes);
        for (int i = 0; i < tierTypes.length; i++) {
            storageTiers[i] = createStorageTier(tierTypes[i]);
        }
    }

    private StorageTier createStorageTier(TierType tierType) throws IOException {
        StorageTier storageTier;
        switch (tierType) {
            case IN_MEM:
                storageTier = getMemoryTier();
                break;
            case IN_DISK:
                storageTier = getDiskTier();
                break;
            case IN_REMOTE:
                storageTier = getRemoteTier();
                break;
            default:
                throw new IllegalArgumentException("No such tier type " + tierType);
        }
        storageTier.setup();
        return storageTier;
    }

    private void addTierExclusiveBuffers(TierType... toAddTierTypes) {
        for (TierType toAddTierType : toAddTierTypes) {
            tierExclusiveBuffers.put(
                    toAddTierType,
                    checkNotNull(HYBRID_SHUFFLE_TIER_EXCLUSIVE_BUFFERS.get(toAddTierType)));
        }
        tieredStoreMemoryManager =
                new UpstreamTieredStoreMemoryManager(
                        tierExclusiveBuffers,
                        numSubpartitions,
                        numBuffersTriggerFlushRatio,
                        cacheFlushManager);
    }

    private MemoryTier getMemoryTier() {
        return new MemoryTier(
                numSubpartitions,
                bufferSize,
                tieredStoreMemoryManager,
                isBroadcast,
                bufferCompressor);
    }

    private DiskTier getDiskTier() {
        return new DiskTier(
                numSubpartitions,
                bufferSize,
                resultPartitionID,
                tieredStoreMemoryManager,
                cacheFlushManager,
                dataFileBasePath,
                minReservedDiskSpaceFraction,
                isBroadcast,
                bufferCompressor,
                partitionFileManager);
    }

    private RemoteTier getRemoteTier() {
        if (StringUtils.isNullOrWhitespaceOnly(baseRemoteStoragePath)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Must specify DFS home path by %s when using DFS in Tiered Store.",
                            NettyShuffleEnvironmentOptions
                                    .NETWORK_HYBRID_SHUFFLE_REMOTE_STORAGE_BASE_HOME_PATH
                                    .key()));
        }
        return new RemoteTier(
                numSubpartitions,
                bufferSize,
                tieredStoreMemoryManager,
                cacheFlushManager,
                isBroadcast,
                bufferCompressor,
                partitionFileManager);
    }
}
