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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.memory.MemorySegmentProvider;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.IndexRange;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.TaskEventPublisher;
import org.apache.flink.runtime.io.network.buffer.BufferDecompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferPoolFactory;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.metrics.InputChannelMetrics;
import org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageIdMappingUtils;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.HashPartitionFile;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.PartitionFileReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageConsumerClient;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageConsumerSpec;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.remote.RemoteStorageScanner;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;
import org.apache.flink.runtime.shuffle.NettyShuffleUtils;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleIOOwnerContext;
import org.apache.flink.runtime.taskmanager.NettyShuffleEnvironmentConfiguration;
import org.apache.flink.runtime.throughput.BufferDebloatConfiguration;
import org.apache.flink.runtime.throughput.BufferDebloater;
import org.apache.flink.runtime.throughput.ThroughputCalculator;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.clock.SystemClock;
import org.apache.flink.util.function.SupplierWithException;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.runtime.io.network.partition.consumer.InputGateSpecUtils.createGateBuffersSpec;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageUtils.getTieredStoragePath;
import static org.apache.flink.runtime.shuffle.ShuffleUtils.applyWithShuffleTypeCheck;

/** Factory for {@link SingleInputGate} to use in {@link NettyShuffleEnvironment}. */
public class SingleInputGateFactory {
    private static final Logger LOG = LoggerFactory.getLogger(SingleInputGateFactory.class);

    @Nonnull protected final ResourceID taskExecutorResourceId;

    protected final int partitionRequestInitialBackoff;

    protected final int partitionRequestMaxBackoff;

    @Nonnull protected final ConnectionManager connectionManager;

    @Nonnull protected final ResultPartitionManager partitionManager;

    @Nonnull protected final TaskEventPublisher taskEventPublisher;

    @Nonnull protected final NetworkBufferPool networkBufferPool;

    protected final Optional<Integer> maxRequiredBuffersPerGate;

    protected final boolean enableTieredStoreForHybridShuffle;

    protected final int configuredNetworkBuffersPerChannel;

    protected final int floatingNetworkBuffersPerGate;

    protected final boolean batchShuffleCompressionEnabled;

    protected final String compressionCodec;

    protected final int networkBufferSize;

    private final BufferDebloatConfiguration debloatConfiguration;

    private final Boolean enableTieredStore;

    @Nullable private final String baseRemoteStoragePath;

    public SingleInputGateFactory(
            @Nonnull ResourceID taskExecutorResourceId,
            @Nonnull NettyShuffleEnvironmentConfiguration networkConfig,
            @Nonnull ConnectionManager connectionManager,
            @Nonnull ResultPartitionManager partitionManager,
            @Nonnull TaskEventPublisher taskEventPublisher,
            @Nonnull NetworkBufferPool networkBufferPool) {
        this.taskExecutorResourceId = taskExecutorResourceId;
        this.partitionRequestInitialBackoff = networkConfig.partitionRequestInitialBackoff();
        this.partitionRequestMaxBackoff = networkConfig.partitionRequestMaxBackoff();
        this.maxRequiredBuffersPerGate = networkConfig.maxRequiredBuffersPerGate();
        this.enableTieredStoreForHybridShuffle = networkConfig.enableTieredStoreForHybridShuffle();
        this.configuredNetworkBuffersPerChannel =
                NettyShuffleUtils.getNetworkBuffersPerInputChannel(
                        networkConfig.networkBuffersPerChannel());
        this.floatingNetworkBuffersPerGate = networkConfig.floatingNetworkBuffersPerGate();
        this.batchShuffleCompressionEnabled = networkConfig.isBatchShuffleCompressionEnabled();
        this.compressionCodec = networkConfig.getCompressionCodec();
        this.networkBufferSize = networkConfig.networkBufferSize();
        this.connectionManager = connectionManager;
        this.partitionManager = partitionManager;
        this.taskEventPublisher = taskEventPublisher;
        this.networkBufferPool = networkBufferPool;
        this.debloatConfiguration = networkConfig.getDebloatConfiguration();
        this.enableTieredStore = networkConfig.enableTieredStoreForHybridShuffle();
        this.baseRemoteStoragePath = networkConfig.getBaseRemoteStoragePath();
    }

    /** Creates an input gate and all of its input channels. */
    public SingleInputGate create(
            @Nonnull ShuffleIOOwnerContext owner,
            int gateIndex,
            @Nonnull InputGateDeploymentDescriptor igdd,
            @Nonnull PartitionProducerStateProvider partitionProducerStateProvider,
            @Nonnull InputChannelMetrics metrics,
            @Nonnull TieredStorageNettyService nettyService) {
        GateBuffersSpec gateBuffersSpec =
                createGateBuffersSpec(
                        maxRequiredBuffersPerGate,
                        configuredNetworkBuffersPerChannel,
                        floatingNetworkBuffersPerGate,
                        igdd.getConsumedPartitionType(),
                        calculateNumChannels(
                                igdd.getShuffleDescriptors().length,
                                igdd.getConsumedSubpartitionIndexRange()),
                        enableTieredStoreForHybridShuffle);
        SupplierWithException<BufferPool, IOException> bufferPoolFactory =
                createBufferPoolFactory(
                        networkBufferPool,
                        gateBuffersSpec.getRequiredFloatingBuffers(),
                        gateBuffersSpec.getTotalFloatingBuffers());

        BufferDecompressor bufferDecompressor = null;
        if (igdd.getConsumedPartitionType().supportCompression()
                && batchShuffleCompressionEnabled) {
            bufferDecompressor = new BufferDecompressor(networkBufferSize, compressionCodec);
        }

        final String owningTaskName = owner.getOwnerName();
        final MetricGroup networkInputGroup = owner.getInputGroup();

        IndexRange subpartitionIndexRange = igdd.getConsumedSubpartitionIndexRange();
        int numberOfInputChannels =
                calculateNumChannels(igdd.getShuffleDescriptors().length, subpartitionIndexRange);

        ShuffleDescriptor[] shuffleDescriptors = igdd.getShuffleDescriptors();
        RemoteStorageScanner remoteStorageScanner = null;
        List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs = null;
        TieredStorageConsumerClient tieredStorageConsumerClient = null;
        PartitionFileReader reader = null;
        if (enableTieredStore) {
            tieredStorageConsumerSpecs = new ArrayList<>();
            for (ShuffleDescriptor shuffleDescriptor : shuffleDescriptors) {
                TieredStoragePartitionId partitionId =
                        TieredStorageIdMappingUtils.convertId(
                                shuffleDescriptor.getResultPartitionID());
                for (int index = subpartitionIndexRange.getStartIndex();
                        index <= subpartitionIndexRange.getEndIndex();
                        ++index) {
                    TieredStorageSubpartitionId subpartitionId =
                            new TieredStorageSubpartitionId(index);
                    tieredStorageConsumerSpecs.add(
                            new TieredStorageConsumerSpec(partitionId, subpartitionId));
                }
            }
            if (baseRemoteStoragePath != null) {
                String basePath = getTieredStoragePath(baseRemoteStoragePath);
                remoteStorageScanner =
                        new RemoteStorageScanner(tieredStorageConsumerSpecs, basePath);
                reader = HashPartitionFile.createPartitionFileReader(basePath);
            }

            tieredStorageConsumerClient =
                    new TieredStorageConsumerClient(
                            tieredStorageConsumerSpecs,
                            nettyService,
                            baseRemoteStoragePath,
                            remoteStorageScanner,
                            reader,
                            networkBufferSize);
        }

        SingleInputGate inputGate =
                createInputGate(
                        owningTaskName,
                        gateIndex,
                        igdd.getConsumedResultId(),
                        igdd.getConsumedPartitionType(),
                        subpartitionIndexRange,
                        numberOfInputChannels,
                        partitionProducerStateProvider,
                        bufferPoolFactory,
                        bufferDecompressor,
                        networkBufferPool,
                        networkBufferSize,
                        new ThroughputCalculator(SystemClock.getInstance()),
                        maybeCreateBufferDebloater(
                                owningTaskName, gateIndex, networkInputGroup.addGroup(gateIndex)),
                        tieredStorageConsumerSpecs,
                        nettyService,
                        baseRemoteStoragePath,
                        remoteStorageScanner,
                        tieredStorageConsumerClient);

        createInputChannels(
                owningTaskName, igdd, inputGate, subpartitionIndexRange, gateBuffersSpec, metrics);
        return inputGate;
    }

    protected BufferDebloater maybeCreateBufferDebloater(
            String owningTaskName, int gateIndex, MetricGroup inputGroup) {
        if (debloatConfiguration.isEnabled()) {
            final BufferDebloater bufferDebloater =
                    new BufferDebloater(
                            owningTaskName,
                            gateIndex,
                            debloatConfiguration.getTargetTotalBufferSize().toMillis(),
                            debloatConfiguration.getMaxBufferSize(),
                            debloatConfiguration.getMinBufferSize(),
                            debloatConfiguration.getBufferDebloatThresholdPercentages(),
                            debloatConfiguration.getNumberOfSamples());
            inputGroup.gauge(
                    MetricNames.ESTIMATED_TIME_TO_CONSUME_BUFFERS,
                    () -> bufferDebloater.getLastEstimatedTimeToConsumeBuffers().toMillis());
            inputGroup.gauge(MetricNames.DEBLOATED_BUFFER_SIZE, bufferDebloater::getLastBufferSize);
            return bufferDebloater;
        }

        return null;
    }

    protected void createInputChannels(
            String owningTaskName,
            InputGateDeploymentDescriptor inputGateDeploymentDescriptor,
            SingleInputGate inputGate,
            IndexRange subpartitionIndexRange,
            GateBuffersSpec gateBuffersSpec,
            InputChannelMetrics metrics) {
        ShuffleDescriptor[] shuffleDescriptors =
                inputGateDeploymentDescriptor.getShuffleDescriptors();

        // Create the input channels. There is one input channel for each consumed subpartition.
        InputChannel[] inputChannels =
                new InputChannel
                        [calculateNumChannels(shuffleDescriptors.length, subpartitionIndexRange)];

        ChannelStatistics channelStatistics = new ChannelStatistics();

        int channelIdx = 0;
        for (int i = 0; i < shuffleDescriptors.length; ++i) {
            for (int subpartitionIndex = subpartitionIndexRange.getStartIndex();
                    subpartitionIndex <= subpartitionIndexRange.getEndIndex();
                    ++subpartitionIndex) {
                inputChannels[channelIdx] =
                        createInputChannel(
                                inputGate,
                                channelIdx,
                                gateBuffersSpec.getEffectiveExclusiveBuffersPerChannel(),
                                shuffleDescriptors[i],
                                subpartitionIndex,
                                channelStatistics,
                                metrics);
                channelIdx++;
            }
        }

        inputGate.setInputChannels(inputChannels);

        LOG.debug(
                "{}: Created {} input channels ({}).",
                owningTaskName,
                inputChannels.length,
                channelStatistics);
    }

    private InputChannel createInputChannel(
            SingleInputGate inputGate,
            int index,
            int buffersPerChannel,
            ShuffleDescriptor shuffleDescriptor,
            int consumedSubpartitionIndex,
            ChannelStatistics channelStatistics,
            InputChannelMetrics metrics) {
        return applyWithShuffleTypeCheck(
                NettyShuffleDescriptor.class,
                shuffleDescriptor,
                unknownShuffleDescriptor -> {
                    channelStatistics.numUnknownChannels++;
                    return new UnknownInputChannel(
                            inputGate,
                            index,
                            unknownShuffleDescriptor.getResultPartitionID(),
                            consumedSubpartitionIndex,
                            partitionManager,
                            taskEventPublisher,
                            connectionManager,
                            partitionRequestInitialBackoff,
                            partitionRequestMaxBackoff,
                            buffersPerChannel,
                            metrics);
                },
                nettyShuffleDescriptor ->
                        createKnownInputChannel(
                                inputGate,
                                index,
                                buffersPerChannel,
                                nettyShuffleDescriptor,
                                consumedSubpartitionIndex,
                                channelStatistics,
                                metrics));
    }

    protected SingleInputGate createInputGate(
            String owningTaskName,
            int gateIndex,
            IntermediateDataSetID consumedResultId,
            final ResultPartitionType consumedPartitionType,
            IndexRange subpartitionIndexRange,
            int numberOfInputChannels,
            PartitionProducerStateProvider partitionProducerStateProvider,
            SupplierWithException<BufferPool, IOException> bufferPoolFactory,
            @Nullable BufferDecompressor bufferDecompressor,
            MemorySegmentProvider memorySegmentProvider,
            int segmentSize,
            ThroughputCalculator throughputCalculator,
            @Nullable BufferDebloater bufferDebloater,
            List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs,
            TieredStorageNettyService nettyService,
            @Nullable String baseRemoteStoragePath,
            @Nullable RemoteStorageScanner remoteStorageScanner,
            @Nullable TieredStorageConsumerClient tieredStorageConsumerClient) {

        return new SingleInputGate(
                owningTaskName,
                gateIndex,
                consumedResultId,
                consumedPartitionType,
                subpartitionIndexRange,
                numberOfInputChannels,
                partitionProducerStateProvider,
                bufferPoolFactory,
                bufferDecompressor,
                memorySegmentProvider,
                segmentSize,
                throughputCalculator,
                bufferDebloater,
                enableTieredStore,
                tieredStorageConsumerSpecs,
                nettyService,
                baseRemoteStoragePath,
                remoteStorageScanner,
                tieredStorageConsumerClient);
    }

    protected static int calculateNumChannels(
            int numShuffleDescriptors, IndexRange subpartitionIndexRange) {
        return MathUtils.checkedDownCast(
                ((long) numShuffleDescriptors) * subpartitionIndexRange.size());
    }

    @VisibleForTesting
    protected InputChannel createKnownInputChannel(
            SingleInputGate inputGate,
            int index,
            int buffersPerChannel,
            NettyShuffleDescriptor inputChannelDescriptor,
            int consumedSubpartitionIndex,
            ChannelStatistics channelStatistics,
            InputChannelMetrics metrics) {
        ResultPartitionID partitionId = inputChannelDescriptor.getResultPartitionID();
        if (inputChannelDescriptor.isLocalTo(taskExecutorResourceId)) {
            // Consuming task is deployed to the same TaskManager as the partition => local
            channelStatistics.numLocalChannels++;
            return new LocalRecoveredInputChannel(
                    inputGate,
                    index,
                    partitionId,
                    consumedSubpartitionIndex,
                    partitionManager,
                    taskEventPublisher,
                    partitionRequestInitialBackoff,
                    partitionRequestMaxBackoff,
                    buffersPerChannel,
                    metrics,
                    inputChannelDescriptor.isUpstreamBroadcastOnly());
        } else {
            // Different instances => remote
            channelStatistics.numRemoteChannels++;
            return new RemoteRecoveredInputChannel(
                    inputGate,
                    index,
                    partitionId,
                    consumedSubpartitionIndex,
                    inputChannelDescriptor.getConnectionId(),
                    connectionManager,
                    partitionRequestInitialBackoff,
                    partitionRequestMaxBackoff,
                    buffersPerChannel,
                    metrics,
                    inputChannelDescriptor.isUpstreamBroadcastOnly());
        }
    }

    @VisibleForTesting
    protected static SupplierWithException<BufferPool, IOException> createBufferPoolFactory(
            BufferPoolFactory bufferPoolFactory, int floatingNetworkBuffersPerGate) {
        Pair<Integer, Integer> pair =
                NettyShuffleUtils.getMinMaxFloatingBuffersPerInputGate(
                        floatingNetworkBuffersPerGate);
        return () -> bufferPoolFactory.createBufferPool(pair.getLeft(), pair.getRight());
    }

    @VisibleForTesting
    public static SupplierWithException<BufferPool, IOException> createBufferPoolFactory(
            BufferPoolFactory bufferPoolFactory,
            int minFloatingBuffersPerGate,
            int maxFloatingBuffersPerGate) {
        Pair<Integer, Integer> pair = Pair.of(minFloatingBuffersPerGate, maxFloatingBuffersPerGate);
        return () -> bufferPoolFactory.createBufferPool(pair.getLeft(), pair.getRight());
    }

    /** Statistics of input channels. */
    protected static class ChannelStatistics {
        int numLocalChannels;
        int numRemoteChannels;
        int numUnknownChannels;

        @Override
        public String toString() {
            return String.format(
                    "local: %s, remote: %s, unknown: %s",
                    numLocalChannels, numRemoteChannels, numUnknownChannels);
        }
    }
}
