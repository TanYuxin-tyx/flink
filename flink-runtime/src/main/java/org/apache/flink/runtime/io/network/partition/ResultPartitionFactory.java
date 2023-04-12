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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.disk.FileChannelManager;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferPoolFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.HsResultPartition;
import org.apache.flink.runtime.io.network.partition.hybrid.HybridShuffleConfiguration;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.RemoteTieredStorageFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.TierType;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.TieredStoreShuffleEnvironment;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.UpstreamTieredStorageFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.TieredStoreConfiguration;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.TieredStoreResultPartition;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.cache.BufferAccumulator;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.cache.BufferAccumulatorImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.CacheFlushManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.TierStorage;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.UpstreamTieredStoreMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.file.PartitionFileManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.file.PartitionFileManagerImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.tier.local.disk.RegionBufferIndexTrackerImpl;
import org.apache.flink.runtime.shuffle.NettyShuffleUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.ProcessorArchitecture;
import org.apache.flink.util.function.SupplierWithException;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.TieredStoreUtils.DATA_FILE_SUFFIX;
import static org.apache.flink.runtime.shuffle.NettyShuffleUtils.HYBRID_SHUFFLE_TIER_EXCLUSIVE_BUFFERS;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** Factory for {@link ResultPartition} to use in {@link NettyShuffleEnvironment}. */
public class ResultPartitionFactory {

    private static final Logger LOG = LoggerFactory.getLogger(ResultPartitionFactory.class);

    private final ResultPartitionManager partitionManager;

    private final FileChannelManager channelManager;

    private final BufferPoolFactory bufferPoolFactory;

    private final BatchShuffleReadBufferPool batchShuffleReadBufferPool;

    private final ScheduledExecutorService batchShuffleReadIOExecutor;

    private final BoundedBlockingSubpartitionType blockingSubpartitionType;

    private final int configuredNetworkBuffersPerChannel;

    private final int floatingNetworkBuffersPerGate;

    private final int networkBufferSize;

    private final boolean batchShuffleCompressionEnabled;

    private final String compressionCodec;

    private final int maxBuffersPerChannel;

    private final int sortShuffleMinBuffers;

    private final int sortShuffleMinParallelism;

    private final int hybridShuffleSpilledIndexSegmentSize;

    private final long hybridShuffleNumRetainedInMemoryRegionsMax;

    private final boolean sslEnabled;

    private final int maxOverdraftBuffersPerGate;

    private final String baseRemoteStorageHomePath;

    private final float minReservedDiskSpaceFraction;

    private final boolean enableTieredStoreForHybridShuffle;

    private final String tieredStoreTiers;

    public ResultPartitionFactory(
            ResultPartitionManager partitionManager,
            FileChannelManager channelManager,
            BufferPoolFactory bufferPoolFactory,
            BatchShuffleReadBufferPool batchShuffleReadBufferPool,
            ScheduledExecutorService batchShuffleReadIOExecutor,
            BoundedBlockingSubpartitionType blockingSubpartitionType,
            int configuredNetworkBuffersPerChannel,
            int floatingNetworkBuffersPerGate,
            int networkBufferSize,
            boolean batchShuffleCompressionEnabled,
            String compressionCodec,
            int maxBuffersPerChannel,
            int sortShuffleMinBuffers,
            int sortShuffleMinParallelism,
            boolean sslEnabled,
            int maxOverdraftBuffersPerGate,
            int hybridShuffleSpilledIndexSegmentSize,
            long hybridShuffleNumRetainedInMemoryRegionsMax,
            String baseRemoteStorageHomePath,
            float minReservedDiskSpaceFraction,
            boolean enableTieredStoreForHybridShuffle,
            String tieredStoreTiers) {

        this.partitionManager = partitionManager;
        this.channelManager = channelManager;
        this.configuredNetworkBuffersPerChannel = configuredNetworkBuffersPerChannel;
        this.floatingNetworkBuffersPerGate = floatingNetworkBuffersPerGate;
        this.bufferPoolFactory = bufferPoolFactory;
        this.batchShuffleReadBufferPool = batchShuffleReadBufferPool;
        this.batchShuffleReadIOExecutor = batchShuffleReadIOExecutor;
        this.blockingSubpartitionType = blockingSubpartitionType;
        this.networkBufferSize = networkBufferSize;
        this.batchShuffleCompressionEnabled = batchShuffleCompressionEnabled;
        this.compressionCodec = compressionCodec;
        this.maxBuffersPerChannel = maxBuffersPerChannel;
        this.sortShuffleMinBuffers = sortShuffleMinBuffers;
        this.sortShuffleMinParallelism = sortShuffleMinParallelism;
        this.sslEnabled = sslEnabled;
        this.maxOverdraftBuffersPerGate = maxOverdraftBuffersPerGate;
        this.hybridShuffleSpilledIndexSegmentSize = hybridShuffleSpilledIndexSegmentSize;
        this.hybridShuffleNumRetainedInMemoryRegionsMax =
                hybridShuffleNumRetainedInMemoryRegionsMax;
        this.baseRemoteStorageHomePath = baseRemoteStorageHomePath;
        this.minReservedDiskSpaceFraction = minReservedDiskSpaceFraction;
        this.tieredStoreTiers = tieredStoreTiers;
        this.enableTieredStoreForHybridShuffle = enableTieredStoreForHybridShuffle;
    }

    public ResultPartition create(
            JobID jobID,
            String taskNameWithSubtaskAndId,
            int partitionIndex,
            ResultPartitionDeploymentDescriptor desc) {
        return create(
                jobID,
                taskNameWithSubtaskAndId,
                partitionIndex,
                desc.getShuffleDescriptor().getResultPartitionID(),
                desc.getPartitionType(),
                desc.getNumberOfSubpartitions(),
                desc.getMaxParallelism(),
                desc.isBroadcast(),
                createBufferPoolFactory(desc.getNumberOfSubpartitions(), desc.getPartitionType()));
    }

    @VisibleForTesting
    public ResultPartition create(
            JobID jobID,
            String taskNameWithSubtaskAndId,
            int partitionIndex,
            ResultPartitionID id,
            ResultPartitionType type,
            int numberOfSubpartitions,
            int maxParallelism,
            boolean isBroadcast,
            SupplierWithException<BufferPool, IOException> bufferPoolFactory) {
        BufferCompressor bufferCompressor = null;
        if (type.supportCompression() && batchShuffleCompressionEnabled) {
            bufferCompressor = new BufferCompressor(networkBufferSize, compressionCodec);
        }

        ResultSubpartition[] subpartitions = new ResultSubpartition[numberOfSubpartitions];

        final ResultPartition partition;
        if (type == ResultPartitionType.PIPELINED
                || type == ResultPartitionType.PIPELINED_BOUNDED
                || type == ResultPartitionType.PIPELINED_APPROXIMATE) {
            final PipelinedResultPartition pipelinedPartition =
                    new PipelinedResultPartition(
                            taskNameWithSubtaskAndId,
                            partitionIndex,
                            id,
                            type,
                            subpartitions,
                            maxParallelism,
                            partitionManager,
                            bufferCompressor,
                            bufferPoolFactory);

            for (int i = 0; i < subpartitions.length; i++) {
                if (type == ResultPartitionType.PIPELINED_APPROXIMATE) {
                    subpartitions[i] =
                            new PipelinedApproximateSubpartition(
                                    i, configuredNetworkBuffersPerChannel, pipelinedPartition);
                } else {
                    subpartitions[i] =
                            new PipelinedSubpartition(
                                    i, configuredNetworkBuffersPerChannel, pipelinedPartition);
                }
            }

            partition = pipelinedPartition;
        } else if (type == ResultPartitionType.BLOCKING
                || type == ResultPartitionType.BLOCKING_PERSISTENT) {
            if (numberOfSubpartitions >= sortShuffleMinParallelism) {
                partition =
                        new SortMergeResultPartition(
                                taskNameWithSubtaskAndId,
                                partitionIndex,
                                id,
                                type,
                                subpartitions.length,
                                maxParallelism,
                                batchShuffleReadBufferPool,
                                batchShuffleReadIOExecutor,
                                partitionManager,
                                channelManager.createChannel().getPath(),
                                bufferCompressor,
                                bufferPoolFactory);
            } else {
                final BoundedBlockingResultPartition blockingPartition =
                        new BoundedBlockingResultPartition(
                                taskNameWithSubtaskAndId,
                                partitionIndex,
                                id,
                                type,
                                subpartitions,
                                maxParallelism,
                                partitionManager,
                                bufferCompressor,
                                bufferPoolFactory);

                initializeBoundedBlockingPartitions(
                        subpartitions,
                        blockingPartition,
                        blockingSubpartitionType,
                        networkBufferSize,
                        channelManager,
                        sslEnabled);

                partition = blockingPartition;
            }
        } else if (type == ResultPartitionType.HYBRID_FULL
                || type == ResultPartitionType.HYBRID_SELECTIVE) {

            if (enableTieredStoreForHybridShuffle) {
                TieredStoreConfiguration storeConfiguration =
                        getStoreConfiguration(numberOfSubpartitions, type);
                CacheFlushManager cacheFlushManager = new CacheFlushManager();
                TieredStoreShuffleEnvironment storeShuffleEnvironment =
                        new TieredStoreShuffleEnvironment(jobID, baseRemoteStorageHomePath);
                UpstreamTieredStoreMemoryManager storeMemoryManager =
                        createStoreMemoryManager(
                                subpartitions.length,
                                storeConfiguration.getTierTypes(),
                                storeConfiguration.getNumBuffersTriggerFlushRatio(),
                                cacheFlushManager);
                TierStorage[] tierStorages =
                        createTierStorages(
                                jobID,
                                id,
                                isBroadcast,
                                bufferCompressor,
                                subpartitions,
                                storeConfiguration,
                                storeShuffleEnvironment,
                                storeMemoryManager,
                                cacheFlushManager);

                BufferAccumulator bufferAccumulator =
                        new BufferAccumulatorImpl(
                                tierStorages,
                                subpartitions.length,
                                networkBufferSize,
                                isBroadcast,
                                storeMemoryManager,
                                bufferCompressor);
                partition =
                        new TieredStoreResultPartition(
                                taskNameWithSubtaskAndId,
                                partitionIndex,
                                id,
                                type,
                                subpartitions.length,
                                maxParallelism,
                                partitionManager,
                                isBroadcast,
                                tierStorages,
                                storeMemoryManager,
                                cacheFlushManager,
                                bufferCompressor,
                                bufferAccumulator,
                                bufferPoolFactory);
            } else {
                partition =
                        new HsResultPartition(
                                taskNameWithSubtaskAndId,
                                partitionIndex,
                                id,
                                type,
                                subpartitions.length,
                                maxParallelism,
                                batchShuffleReadBufferPool,
                                batchShuffleReadIOExecutor,
                                partitionManager,
                                channelManager.createChannel().getPath(),
                                networkBufferSize,
                                getHybridShuffleConfiguration(numberOfSubpartitions, type),
                                bufferCompressor,
                                isBroadcast,
                                bufferPoolFactory);
            }

        } else {
            throw new IllegalArgumentException("Unrecognized ResultPartitionType: " + type);
        }

        LOG.debug("{}: Initialized {}", taskNameWithSubtaskAndId, this);

        return partition;
    }

    private TierStorage[] createTierStorages(
            JobID jobID,
            ResultPartitionID id,
            boolean isBroadcast,
            BufferCompressor bufferCompressor,
            ResultSubpartition[] subpartitions,
            TieredStoreConfiguration storeConfiguration,
            TieredStoreShuffleEnvironment storeShuffleEnvironment,
            UpstreamTieredStoreMemoryManager storeMemoryManager,
            CacheFlushManager cacheFlushManager) {
        String dataFileBasePath = channelManager.createChannel().getPath();
        PartitionFileManager partitionFileManager =
                new PartitionFileManagerImpl(
                        Paths.get(dataFileBasePath + DATA_FILE_SUFFIX),
                        new RegionBufferIndexTrackerImpl(isBroadcast ? 1 : subpartitions.length),
                        batchShuffleReadBufferPool,
                        batchShuffleReadIOExecutor,
                        storeConfiguration,
                        subpartitions.length,
                        jobID,
                        id,
                        storeConfiguration.getBaseDfsHomePath());
        UpstreamTieredStorageFactory upstreamTieredStorageFactory = null;
        if (storeConfiguration.getUpstreamTierTypes().length > 0) {
            upstreamTieredStorageFactory =
                    storeShuffleEnvironment.createUpstreamTieredStorageFactory(
                            storeConfiguration.getUpstreamTierTypes(),
                            id,
                            subpartitions.length,
                            networkBufferSize,
                            storeConfiguration.getNumBuffersTriggerFlushRatio(),
                            minReservedDiskSpaceFraction,
                            dataFileBasePath,
                            isBroadcast,
                            batchShuffleReadBufferPool,
                            batchShuffleReadIOExecutor,
                            bufferCompressor,
                            partitionFileManager,
                            storeMemoryManager,
                            storeConfiguration,
                            cacheFlushManager);
        }
        RemoteTieredStorageFactory remoteTieredStorageFactory = null;
        if (storeConfiguration.getRemoteTierTypes().length > 0) {
            remoteTieredStorageFactory =
                    storeShuffleEnvironment.createRemoteTieredStorageFactory(
                            new TierType[] {TierType.IN_REMOTE},
                            id,
                            subpartitions.length,
                            networkBufferSize,
                            storeConfiguration.getNumBuffersTriggerFlushRatio(),
                            minReservedDiskSpaceFraction,
                            dataFileBasePath,
                            isBroadcast,
                            batchShuffleReadBufferPool,
                            batchShuffleReadIOExecutor,
                            bufferCompressor,
                            partitionFileManager,
                            storeMemoryManager,
                            storeConfiguration,
                            cacheFlushManager);
        }
        checkState(upstreamTieredStorageFactory != null || remoteTieredStorageFactory != null);
        TierStorage[] tierStorages;
        if (upstreamTieredStorageFactory == null) {
            tierStorages = remoteTieredStorageFactory.getTierStorages();
        } else if (remoteTieredStorageFactory == null) {
            tierStorages = upstreamTieredStorageFactory.getTierStorages();
        } else {
            TierStorage[] upstreamTierStorages = upstreamTieredStorageFactory.getTierStorages();
            TierStorage[] remoteTierStorages = remoteTieredStorageFactory.getTierStorages();
            tierStorages = new TierStorage[upstreamTierStorages.length + remoteTierStorages.length];
            System.arraycopy(upstreamTierStorages, 0, tierStorages, 0, upstreamTierStorages.length);
            System.arraycopy(
                    remoteTierStorages,
                    0,
                    tierStorages,
                    upstreamTierStorages.length,
                    remoteTierStorages.length);
        }
        return tierStorages;
    }

    private UpstreamTieredStoreMemoryManager createStoreMemoryManager(
            int numSubpartitions,
            TierType[] tierTypes,
            float numBuffersTriggerFlushRatio,
            CacheFlushManager cacheFlushManager) {
        Map<TierType, Integer> tierExclusiveBuffers = new HashMap<>();
        for (TierType tierType : tierTypes) {
            tierExclusiveBuffers.put(
                    tierType, checkNotNull(HYBRID_SHUFFLE_TIER_EXCLUSIVE_BUFFERS.get(tierType)));
        }
        return new UpstreamTieredStoreMemoryManager(
                tierExclusiveBuffers,
                numSubpartitions,
                numBuffersTriggerFlushRatio,
                cacheFlushManager);
    }

    private HybridShuffleConfiguration getHybridShuffleConfiguration(
            int numberOfSubpartitions, ResultPartitionType resultPartitionType) {
        return HybridShuffleConfiguration.builder(
                        numberOfSubpartitions, batchShuffleReadBufferPool.getNumBuffersPerRequest())
                .setSpillingStrategyType(
                        resultPartitionType == ResultPartitionType.HYBRID_FULL
                                ? HybridShuffleConfiguration.SpillingStrategyType.FULL
                                : HybridShuffleConfiguration.SpillingStrategyType.SELECTIVE)
                .setSpilledIndexSegmentSize(hybridShuffleSpilledIndexSegmentSize)
                .setNumRetainedInMemoryRegionsMax(hybridShuffleNumRetainedInMemoryRegionsMax)
                .build();
    }

    private TieredStoreConfiguration getStoreConfiguration(
            int numberOfSubpartitions, ResultPartitionType type) {

        return TieredStoreConfiguration.builder(
                        numberOfSubpartitions, batchShuffleReadBufferPool.getNumBuffersPerRequest())
                .setTierTypes(tieredStoreTiers, type)
                .setBaseDfsHomePath(baseRemoteStorageHomePath)
                .setConfiguredNetworkBuffersPerChannel(configuredNetworkBuffersPerChannel)
                .build();
    }

    private static void initializeBoundedBlockingPartitions(
            ResultSubpartition[] subpartitions,
            BoundedBlockingResultPartition parent,
            BoundedBlockingSubpartitionType blockingSubpartitionType,
            int networkBufferSize,
            FileChannelManager channelManager,
            boolean sslEnabled) {
        int i = 0;
        try {
            for (i = 0; i < subpartitions.length; i++) {
                final File spillFile = channelManager.createChannel().getPathFile();
                subpartitions[i] =
                        blockingSubpartitionType.create(
                                i, parent, spillFile, networkBufferSize, sslEnabled);
            }
        } catch (IOException e) {
            // undo all the work so that a failed constructor does not leave any resources
            // in need of disposal
            releasePartitionsQuietly(subpartitions, i);

            // this is not good, we should not be forced to wrap this in a runtime exception.
            // the fact that the ResultPartition and Task constructor (which calls this) do not
            // tolerate any exceptions
            // is incompatible with eager initialization of resources (RAII).
            throw new FlinkRuntimeException(e);
        }
    }

    private static void releasePartitionsQuietly(ResultSubpartition[] partitions, int until) {
        for (int i = 0; i < until; i++) {
            final ResultSubpartition subpartition = partitions[i];
            ExceptionUtils.suppressExceptions(subpartition::release);
        }
    }

    /** Return whether this result partition need overdraft buffer. */
    private static boolean isOverdraftBufferNeeded(ResultPartitionType resultPartitionType) {
        // Only pipelined / pipelined-bounded partition needs overdraft buffer. More
        // specifically, there is no reason to request more buffers for non-pipelined (i.e.
        // batch) shuffle. The reasons are as follows:
        // 1. For BoundedBlockingShuffle, each full buffer will be directly released.
        // 2. For SortMergeShuffle, the maximum capacity of buffer pool is 4 * numSubpartitions. It
        // is efficient enough to spill this part of memory to disk.
        // 3. For Hybrid Shuffle, the buffer pool is unbounded. If it can't get a normal buffer, it
        // also can't get an overdraft buffer.
        return resultPartitionType.isPipelinedOrPipelinedBoundedResultPartition();
    }

    /**
     * The minimum pool size should be <code>numberOfSubpartitions + 1</code> for two
     * considerations:
     *
     * <p>1. StreamTask can only process input if there is at-least one available buffer on output
     * side, so it might cause stuck problem if the minimum pool size is exactly equal to the number
     * of subpartitions, because every subpartition might maintain a partial unfilled buffer.
     *
     * <p>2. Increases one more buffer for every output LocalBufferPool to avoid performance
     * regression if processing input is based on at-least one buffer available on output side.
     */
    @VisibleForTesting
    SupplierWithException<BufferPool, IOException> createBufferPoolFactory(
            int numberOfSubpartitions, ResultPartitionType type) {
        return () -> {
            Pair<Integer, Integer> pair =
                    NettyShuffleUtils.getMinMaxNetworkBuffersPerResultPartition(
                            configuredNetworkBuffersPerChannel,
                            floatingNetworkBuffersPerGate,
                            sortShuffleMinParallelism,
                            sortShuffleMinBuffers,
                            numberOfSubpartitions,
                            enableTieredStoreForHybridShuffle,
                            type);

            return bufferPoolFactory.createBufferPool(
                    pair.getLeft(),
                    pair.getRight(),
                    numberOfSubpartitions,
                    maxBuffersPerChannel,
                    isOverdraftBufferNeeded(type) ? maxOverdraftBuffersPerGate : 0);
        };
    }

    static BoundedBlockingSubpartitionType getBoundedBlockingType() {
        switch (ProcessorArchitecture.getMemoryAddressSize()) {
            case _64_BIT:
                return BoundedBlockingSubpartitionType.FILE_MMAP;
            case _32_BIT:
                return BoundedBlockingSubpartitionType.FILE;
            default:
                LOG.warn("Cannot determine memory architecture. Using pure file-based shuffle.");
                return BoundedBlockingSubpartitionType.FILE;
        }
    }
}
