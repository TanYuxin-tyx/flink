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

package org.apache.flink.runtime.shuffle;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageConfiguration;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMasterClient;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageResourceRegistry;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierMasterAgent;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor.LocalExecutionPartitionConnectionInfo;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor.NetworkPartitionConnectionInfo;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor.PartitionConnectionInfo;
import org.apache.flink.runtime.util.ConfigurationParserUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.NettyShuffleEnvironmentOptions.NETWORK_HYBRID_SHUFFLE_ENABLE_TIERED_STORE;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Default {@link ShuffleMaster} for netty and local file based shuffle implementation. */
public class NettyShuffleMaster implements ShuffleMaster<NettyShuffleDescriptor> {

    private static final Logger LOG = LoggerFactory.getLogger(NettyShuffleMaster.class);

    private final int buffersPerInputChannel;

    private final int floatingBuffersPerGate;

    private final Optional<Integer> maxRequiredBuffersPerGate;

    private final int sortShuffleMinParallelism;

    private final int sortShuffleMinBuffers;

    private final int networkBufferSize;

    @Nullable private TieredStorageConfiguration tieredStorageConfiguration;

    @Nullable private TieredStorageMasterClient tieredStorageMasterClient;


    @Nullable private String baseRemoteStoragePath;

    public NettyShuffleMaster(Configuration conf) {
        checkNotNull(conf);
        buffersPerInputChannel =
                conf.getInteger(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_PER_CHANNEL);
        floatingBuffersPerGate =
                conf.getInteger(NettyShuffleEnvironmentOptions.NETWORK_EXTRA_BUFFERS_PER_GATE);
        maxRequiredBuffersPerGate =
                conf.getOptional(
                        NettyShuffleEnvironmentOptions.NETWORK_READ_MAX_REQUIRED_BUFFERS_PER_GATE);
        sortShuffleMinParallelism =
                conf.getInteger(
                        NettyShuffleEnvironmentOptions.NETWORK_SORT_SHUFFLE_MIN_PARALLELISM);
        sortShuffleMinBuffers =
                conf.getInteger(NettyShuffleEnvironmentOptions.NETWORK_SORT_SHUFFLE_MIN_BUFFERS);
        networkBufferSize = ConfigurationParserUtils.getPageSize(conf);
        checkArgument(
                !maxRequiredBuffersPerGate.isPresent() || maxRequiredBuffersPerGate.get() >= 1,
                String.format(
                        "At least one buffer is required for each gate, please increase the value of %s.",
                        NettyShuffleEnvironmentOptions.NETWORK_READ_MAX_REQUIRED_BUFFERS_PER_GATE
                                .key()));
        checkArgument(
                floatingBuffersPerGate >= 1,
                String.format(
                        "The configured floating buffer should be at least 1, please increase the value of %s.",
                        NettyShuffleEnvironmentOptions.NETWORK_EXTRA_BUFFERS_PER_GATE.key()));

        boolean enableTieredStorage = conf.getBoolean(NETWORK_HYBRID_SHUFFLE_ENABLE_TIERED_STORE);
        if (enableTieredStorage) {
            baseRemoteStoragePath =
                    conf.getString(
                            NettyShuffleEnvironmentOptions
                                    .NETWORK_HYBRID_SHUFFLE_REMOTE_STORAGE_BASE_PATH);
            TieredStorageConfiguration tieredStorageConfiguration =
                    TieredStorageConfiguration.builder(networkBufferSize, baseRemoteStoragePath)
                            .build();
            TieredStorageResourceRegistry resourceRegistry = new TieredStorageResourceRegistry();
            List<TierMasterAgent> tieredMasterClients =
                    createTieredMasterClients(tieredStorageConfiguration, resourceRegistry);
            tieredStorageMasterClient = new TieredStorageMasterClient(tieredMasterClients);
        }
    }

    private List<TierMasterAgent> createTieredMasterClients(
            TieredStorageConfiguration storeConfiguration,
            TieredStorageResourceRegistry resourceRegistry) {
        return storeConfiguration.getTierFactories().stream()
                .map(tierFactory -> tierFactory.createMasterAgent(resourceRegistry))
                .collect(Collectors.toList());
    }

    @Override
    public CompletableFuture<NettyShuffleDescriptor> registerPartitionWithProducer(
            JobID jobID,
            PartitionDescriptor partitionDescriptor,
            ProducerDescriptor producerDescriptor) {

        ResultPartitionID resultPartitionID =
                new ResultPartitionID(
                        partitionDescriptor.getPartitionId(),
                        producerDescriptor.getProducerExecutionId());

        NettyShuffleDescriptor shuffleDeploymentDescriptor =
                new NettyShuffleDescriptor(
                        producerDescriptor.getProducerLocation(),
                        createConnectionInfo(
                                producerDescriptor, partitionDescriptor.getConnectionIndex()),
                        resultPartitionID,
                        partitionDescriptor.isBroadcast());
        if (tieredStorageMasterClient != null) {
            tieredStorageMasterClient.register(resultPartitionID);
        }
        return CompletableFuture.completedFuture(shuffleDeploymentDescriptor);
    }

    @Override
    public void releasePartitionExternally(ShuffleDescriptor shuffleDescriptor) {
        ResultPartitionID resultPartitionID = shuffleDescriptor.getResultPartitionID();
        if (tieredStorageMasterClient != null) {
            tieredStorageMasterClient.release(resultPartitionID);
        }
    }

    private static PartitionConnectionInfo createConnectionInfo(
            ProducerDescriptor producerDescriptor, int connectionIndex) {
        return producerDescriptor.getDataPort() >= 0
                ? NetworkPartitionConnectionInfo.fromProducerDescriptor(
                        producerDescriptor, connectionIndex)
                : LocalExecutionPartitionConnectionInfo.INSTANCE;
    }

    /**
     * JM announces network memory requirement from the calculating result of this method. Please
     * note that the calculating algorithm depends on both I/O details of a vertex and network
     * configuration, e.g. {@link NettyShuffleEnvironmentOptions#NETWORK_BUFFERS_PER_CHANNEL} and
     * {@link NettyShuffleEnvironmentOptions#NETWORK_EXTRA_BUFFERS_PER_GATE}, which means we should
     * always keep the consistency of configurations between JM, RM and TM in fine-grained resource
     * management, thus to guarantee that the processes of memory announcing and allocating respect
     * each other.
     */
    @Override
    public MemorySize computeShuffleMemorySizeForTask(TaskInputsOutputsDescriptor desc) {
        checkNotNull(desc);

        boolean enableTieredStoreForHybridShuffle = tieredStorageConfiguration != null;
        boolean enableRemoteStorageInTieredStore = baseRemoteStoragePath != null;
        int numBuffersUseSortAccumulatorThreshold = 0;
        List<Integer> tieredStorageTierExclusiveBuffers = new ArrayList<>();
        if(enableTieredStoreForHybridShuffle){
            numBuffersUseSortAccumulatorThreshold = tieredStorageConfiguration.getNumBuffersUseSortAccumulatorThreshold();
            tieredStorageTierExclusiveBuffers = tieredStorageConfiguration.getTieredStorageTierExclusiveBuffers();
        }
        int numRequiredNetworkBuffers =
                NettyShuffleUtils.computeNetworkBuffersForAnnouncing(
                        buffersPerInputChannel,
                        floatingBuffersPerGate,
                        maxRequiredBuffersPerGate,
                        sortShuffleMinParallelism,
                        sortShuffleMinBuffers,
                        enableTieredStoreForHybridShuffle,
                        enableRemoteStorageInTieredStore,
                        numBuffersUseSortAccumulatorThreshold,
                        tieredStorageTierExclusiveBuffers,
                        desc.getInputChannelNums(),
                        desc.getPartitionReuseCount(),
                        desc.getSubpartitionNums(),
                        desc.getInputPartitionTypes(),
                        desc.getPartitionTypes());

        return new MemorySize((long) networkBufferSize * numRequiredNetworkBuffers);
    }
}
