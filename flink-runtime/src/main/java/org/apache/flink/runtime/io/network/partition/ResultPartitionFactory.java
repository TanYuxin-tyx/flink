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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.disk.FileChannelManager;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferPoolFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.HsResultPartition;
import org.apache.flink.runtime.io.network.partition.hybrid.HybridShuffleConfiguration;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageConfiguration;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageIdMappingUtils;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyServiceImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.shuffle.TieredResultPartition;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.BufferAccumulator;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.HashBufferAccumulator;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.SortBufferAccumulator;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManagerImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemorySpec;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageProducerClient;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageResourceRegistry;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierProducerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.memory.MemoryTierFactory;
import org.apache.flink.runtime.shuffle.NettyShuffleUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.ProcessorArchitecture;
import org.apache.flink.util.function.SupplierWithException;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.util.Preconditions.checkNotNull;

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

    // The following attributes will be null if tiered storage shuffle is disabled.
    @Nullable private final TieredStorageConfiguration tieredStorageConfiguration;

    @Nullable private final TieredStorageNettyServiceImpl tieredStorageNettyService;

    @Nullable private final TieredStorageResourceRegistry tieredStorageResourceRegistry;

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
            @Nullable TieredStorageConfiguration tieredStorageConfiguration,
            @Nullable TieredStorageNettyServiceImpl tieredStorageNettyService,
            @Nullable TieredStorageResourceRegistry tieredStorageResourceRegistry) {

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
        this.tieredStorageConfiguration = tieredStorageConfiguration;
        this.tieredStorageNettyService = tieredStorageNettyService;
        this.tieredStorageResourceRegistry = tieredStorageResourceRegistry;
    }

    public ResultPartition create(
            String taskNameWithSubtaskAndId,
            int partitionIndex,
            ResultPartitionDeploymentDescriptor desc) {
        return create(
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
            if (tieredStorageConfiguration != null) {
                // Create memory manager.
                TieredStorageMemoryManager memoryManager =
                        new TieredStorageMemoryManagerImpl(
                                checkNotNull(tieredStorageConfiguration)
                                        .getNumBuffersTriggerFlushRatio(),
                                true);

                // Create buffer accumulator.
                int accumulatorExclusiveBufferNum =
                        tieredStorageConfiguration.getAccumulatorExclusiveBuffers();
                BufferAccumulator bufferAccumulator =
                        createBufferAccumulator(
                                numberOfSubpartitions,
                                accumulatorExclusiveBufferNum,
                                memoryManager);

                // Create producer agents and memory specs.
                Tuple2<List<TierProducerAgent>, List<TieredStorageMemorySpec>>
                        producerAgentsAndMemorySpecs =
                                createTierProducerAgentsAndMemorySpecs(
                                        numberOfSubpartitions,
                                        isBroadcast,
                                        TieredStorageIdMappingUtils.convertId(id),
                                        memoryManager,
                                        bufferAccumulator,
                                        type == ResultPartitionType.HYBRID_SELECTIVE);

                // Create producer client.
                TieredStorageProducerClient tieredStorageProducerClient =
                        new TieredStorageProducerClient(
                                numberOfSubpartitions,
                                isBroadcast,
                                bufferAccumulator,
                                bufferCompressor,
                                producerAgentsAndMemorySpecs.f0);

                // Create tiered result partition.
                return new TieredResultPartition(
                        taskNameWithSubtaskAndId,
                        partitionIndex,
                        id,
                        type,
                        numberOfSubpartitions,
                        maxParallelism,
                        partitionManager,
                        bufferCompressor,
                        bufferPoolFactory,
                        tieredStorageProducerClient,
                        tieredStorageResourceRegistry,
                        tieredStorageNettyService,
                        producerAgentsAndMemorySpecs.f1,
                        memoryManager);
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

    private BufferAccumulator createBufferAccumulator(
            int numSubpartitions,
            int accumulatorExclusiveBufferNum,
            TieredStorageMemoryManager storageMemoryManager) {
        int bufferSize = checkNotNull(tieredStorageConfiguration).getTieredStorageBufferSize();
        return (numSubpartitions + 1) > accumulatorExclusiveBufferNum
                ? new SortBufferAccumulator(
                        numSubpartitions,
                        accumulatorExclusiveBufferNum,
                        bufferSize,
                        storageMemoryManager)
                : new HashBufferAccumulator(numSubpartitions, bufferSize, storageMemoryManager);
    }

    private Tuple2<List<TierProducerAgent>, List<TieredStorageMemorySpec>>
            createTierProducerAgentsAndMemorySpecs(
                    int numberOfSubpartitions,
                    boolean isBroadcastOnly,
                    TieredStoragePartitionId partitionID,
                    TieredStorageMemoryManager memoryManager,
                    BufferAccumulator bufferAccumulator,
                    boolean isHybridSelective) {

        List<TierProducerAgent> tierProducerAgents = new ArrayList<>();
        List<TieredStorageMemorySpec> tieredStorageMemorySpecs = new ArrayList<>();

        tieredStorageMemorySpecs.add(
                new TieredStorageMemorySpec(
                        bufferAccumulator,
                        Math.min(
                                numberOfSubpartitions + 1,
                                checkNotNull(tieredStorageConfiguration)
                                        .getAccumulatorExclusiveBuffers())));
        List<Integer> tierExclusiveBuffers =
                tieredStorageConfiguration.getEachTierExclusiveBufferNum();

        List<TierFactory> tierFactories =
                checkNotNull(tieredStorageConfiguration).getTierFactories();
        for (int index = 0; index < tierFactories.size(); ++index) {
            TierFactory tierFactory = tierFactories.get(index);
            if (!isHybridSelective && tierFactory.getClass() == MemoryTierFactory.class) {
                continue;
            }
            TierProducerAgent producerAgent =
                    tierFactory.createProducerAgent(
                            numberOfSubpartitions,
                            partitionID,
                            channelManager.createChannel().getPath(),
                            isBroadcastOnly,
                            memoryManager,
                            checkNotNull(tieredStorageNettyService),
                            checkNotNull(tieredStorageResourceRegistry),
                            batchShuffleReadBufferPool,
                            batchShuffleReadIOExecutor,
                            Math.max(
                                    2 * batchShuffleReadBufferPool.getNumBuffersPerRequest(),
                                    numberOfSubpartitions),
                            checkNotNull(tieredStorageConfiguration)
                                    .getDiskIOSchedulerBufferRequestTimeout(),
                            checkNotNull(tieredStorageConfiguration)
                                    .getDiskIOSchedulerMaxBuffersReadAhead());
            tierProducerAgents.add(producerAgent);
            tieredStorageMemorySpecs.add(
                    new TieredStorageMemorySpec(producerAgent, tierExclusiveBuffers.get(index)));
        }
        return Tuple2.of(tierProducerAgents, tieredStorageMemorySpecs);
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
                            tieredStorageConfiguration != null,
                            tieredStorageConfiguration != null
                                    ? tieredStorageConfiguration.getTotalExclusiveBufferNum()
                                    : 0,
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
