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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.TierType;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.TieredStoreConfiguration;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.file.PartitionFileManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.file.PartitionFileManagerImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.tier.local.disk.DiskTierWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.tier.local.disk.RegionBufferIndexTrackerImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.tier.local.memory.MemoryTierWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.tier.remote.RemoteTierWriter;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.TieredStoreUtils.DATA_FILE_SUFFIX;
import static org.apache.flink.runtime.shuffle.NettyShuffleUtils.HYBRID_SHUFFLE_TIER_EXCLUSIVE_BUFFERS;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class TierWriterFactoryImpl implements TierWriterFactory {

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

    private final TierWriter[] tierWriters;

    public final Map<TierType, Integer> tierExclusiveBuffers;

    private final CacheFlushManager cacheFlushManager;

    private final PartitionFileManager partitionFileManager;

    private TieredStoreMemoryManager tieredStoreMemoryManager;

    public TierWriterFactoryImpl(
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
        this.tierWriters = new TierWriter[tierTypes.length];
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
    }

    @Override
    public TierWriter[] getTierWriters() {
        return tierWriters;
    }

    @Override
    public void setup() {
        addTierExclusiveBuffers(tierTypes);
        try {
            for (int i = 0; i < tierTypes.length; i++) {
                tierWriters[i] = createStorageTier(tierTypes[i]);
            }
        } catch (IOException e) {
            ExceptionUtils.rethrow(e);
        }
    }

    @Override
    public TieredStoreMemoryManager getTieredStoreMemoryManager() {
        return tieredStoreMemoryManager;
    }

    private TierWriter createStorageTier(TierType tierType) throws IOException {
        TierWriter tierWriter;
        switch (tierType) {
            case IN_MEM:
                tierWriter = getMemoryTierWriter();
                break;
            case IN_DISK:
                tierWriter = getDiskTierWriter();
                break;
            case IN_REMOTE:
                tierWriter = getRemoteTierWriter();
                break;
            default:
                throw new IllegalArgumentException("Illegal tier type " + tierType);
        }
        tierWriter.setup();
        return tierWriter;
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

    private MemoryTierWriter getMemoryTierWriter() {
        return new MemoryTierWriter(
                numSubpartitions,
                bufferSize,
                tieredStoreMemoryManager,
                isBroadcast,
                bufferCompressor);
    }

    private DiskTierWriter getDiskTierWriter() {
        return new DiskTierWriter(
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

    private RemoteTierWriter getRemoteTierWriter() {
        if (StringUtils.isNullOrWhitespaceOnly(baseRemoteStoragePath)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Must specify DFS home path by %s when using DFS in Tiered Store.",
                            NettyShuffleEnvironmentOptions
                                    .NETWORK_HYBRID_SHUFFLE_REMOTE_STORAGE_BASE_HOME_PATH
                                    .key()));
        }
        return new RemoteTierWriter(
                numSubpartitions,
                bufferSize,
                tieredStoreMemoryManager,
                cacheFlushManager,
                isBroadcast,
                bufferCompressor,
                partitionFileManager);
    }
}
