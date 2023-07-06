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

import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.GateBuffersSpec;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;

import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.runtime.io.network.partition.consumer.InputGateSpecUtils.createGateBuffersSpec;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Utils to calculate network memory requirement of a vertex from network configuration and details
 * of input and output. The methods help to decide the volume of buffer pools when initializing
 * shuffle environment and also guide network memory announcing in fine-grained resource management.
 */
public class NettyShuffleUtils {

    /**
     * Calculates and returns the number of required exclusive network buffers per input channel.
     */
    public static int getNetworkBuffersPerInputChannel(
            final int configuredNetworkBuffersPerChannel) {
        return configuredNetworkBuffersPerChannel;
    }

    /**
     * Calculates and returns the floating network buffer pool size used by the input gate. The
     * left/right value of the returned pair represent the min/max buffers require by the pool.
     */
    public static Pair<Integer, Integer> getMinMaxFloatingBuffersPerInputGate(
            final int numFloatingBuffersPerGate) {
        // We should guarantee at-least one floating buffer for local channel state recovery.
        return Pair.of(1, numFloatingBuffersPerGate);
    }

    /**
     * Calculates and returns local network buffer pool size used by the result partition. The
     * left/right value of the returned pair represent the min/max buffers require by the pool.
     */
    public static Pair<Integer, Integer> getMinMaxNetworkBuffersPerResultPartition(
            final int configuredNetworkBuffersPerChannel,
            final int numFloatingBuffersPerGate,
            final int sortShuffleMinParallelism,
            final int sortShuffleMinBuffers,
            final int numSubpartitions,
            final boolean enableTieredStoreForHybridShuffle,
            int tieredStorageTotalExclusiveBuffers,
            final ResultPartitionType type) {
        boolean isSortShuffle =
                type.isBlockingOrBlockingPersistentResultPartition()
                        && numSubpartitions >= sortShuffleMinParallelism;
        int min;
        if (isSortShuffle) {
            min = sortShuffleMinBuffers;
        } else {
            if (type.isHybridResultPartition() && enableTieredStoreForHybridShuffle) {
                checkState(
                        tieredStorageTotalExclusiveBuffers >= 0,
                        "Must be non-negative when enable tiered storage.");
                min = Math.min(numSubpartitions + 1, tieredStorageTotalExclusiveBuffers);
            } else {
                min = numSubpartitions + 1;
            }
        }
        int max =
                type.isBounded()
                        ? numSubpartitions * configuredNetworkBuffersPerChannel
                                + numFloatingBuffersPerGate
                        : (isSortShuffle
                                ? Math.max(min, 4 * numSubpartitions)
                                : NetworkBufferPool.UNBOUNDED_POOL_SIZE);
        // for each upstream hash-based blocking/pipelined subpartition, at least one buffer is
        // needed even the configured network buffers per channel is 0 and this behavior is for
        // performance. If it's not guaranteed that each subpartition can get at least one buffer,
        // more partial buffers with little data will be outputted to network/disk and recycled to
        // be used by other subpartitions which can not get a buffer for data caching.
        return Pair.of(min, Math.max(min, max));
    }


    public static int computeNetworkBuffersForAnnouncing(
            final int numBuffersPerChannel,
            final int numFloatingBuffersPerGate,
            final Optional<Integer> maxRequiredBuffersPerGate,
            final int sortShuffleMinParallelism,
            final int sortShuffleMinBuffers,
            final boolean enableTieredStoreForHybridShuffle,
            final boolean enableRemoteStorage,
            final int numBuffersUseSortAccumulatorThreshold,
            final List<Integer> tieredStorageTierExclusiveBuffers,
            final Map<IntermediateDataSetID, Integer> inputChannelNums,
            final Map<IntermediateDataSetID, Integer> partitionReuseCount,
            final Map<IntermediateDataSetID, Integer> subpartitionNums,
            final Map<IntermediateDataSetID, ResultPartitionType> inputPartitionTypes,
            final Map<IntermediateDataSetID, ResultPartitionType> partitionTypes) {

        int requirementForInputs = 0;
        for (IntermediateDataSetID dataSetId : inputChannelNums.keySet()) {
            int numChannels = inputChannelNums.get(dataSetId);
            ResultPartitionType inputPartitionType = inputPartitionTypes.get(dataSetId);
            checkNotNull(inputPartitionType);

            int numSingleGateBuffers =
                    getNumBuffersToAnnounceForInputGate(
                            inputPartitionType,
                            numBuffersPerChannel,
                            numFloatingBuffersPerGate,
                            maxRequiredBuffersPerGate,
                            numChannels,
                            enableTieredStoreForHybridShuffle);
            checkState(partitionReuseCount.containsKey(dataSetId));
            requirementForInputs += numSingleGateBuffers * partitionReuseCount.get(dataSetId);
        }

        int requirementForOutputs = 0;
        for (IntermediateDataSetID dataSetId : subpartitionNums.keySet()) {
            int numSubs = subpartitionNums.get(dataSetId);
            ResultPartitionType partitionType = partitionTypes.get(dataSetId);
            checkNotNull(partitionType);

            requirementForOutputs +=
                    getNumBuffersToAnnounceForResultPartition(
                            partitionType,
                            numBuffersPerChannel,
                            numFloatingBuffersPerGate,
                            sortShuffleMinParallelism,
                            sortShuffleMinBuffers,
                            enableTieredStoreForHybridShuffle,
                            calculateTieredStorageTotalBuffers(enableRemoteStorage, numBuffersUseSortAccumulatorThreshold, partitionType, tieredStorageTierExclusiveBuffers),
                            numSubs);
        }

        return requirementForInputs + requirementForOutputs;
    }

    public static int calculateTieredStorageTotalBuffers(
            boolean enableRemoteStorage,
            int numBuffersUseSortAccumulatorThreshold,
            ResultPartitionType type,
            List<Integer> tieredStorageTierExclusiveBuffers){
        int totalNumBuffer = numBuffersUseSortAccumulatorThreshold;
        if(type == ResultPartitionType.HYBRID_FULL){
            totalNumBuffer += tieredStorageTierExclusiveBuffers.get(1);
            if(enableRemoteStorage){
                totalNumBuffer += tieredStorageTierExclusiveBuffers.get(2);
            }
        }
        if(type == ResultPartitionType.HYBRID_SELECTIVE){
            totalNumBuffer += tieredStorageTierExclusiveBuffers.get(0);
            totalNumBuffer += tieredStorageTierExclusiveBuffers.get(1);
            if(enableRemoteStorage){
                totalNumBuffer += tieredStorageTierExclusiveBuffers.get(2);
            }
        }
        return totalNumBuffer;
    }



    private static int getNumBuffersToAnnounceForInputGate(
            ResultPartitionType type,
            int configuredNetworkBuffersPerChannel,
            int floatingNetworkBuffersPerGate,
            Optional<Integer> maxRequiredBuffersPerGate,
            int numInputChannels,
            boolean enableTieredStoreForHybridShuffle) {
        GateBuffersSpec gateBuffersSpec =
                createGateBuffersSpec(
                        maxRequiredBuffersPerGate,
                        configuredNetworkBuffersPerChannel,
                        floatingNetworkBuffersPerGate,
                        type,
                        numInputChannels,
                        enableTieredStoreForHybridShuffle);
        return gateBuffersSpec.targetTotalBuffersPerGate();
    }

    private static int getNumBuffersToAnnounceForResultPartition(
            ResultPartitionType type,
            int configuredNetworkBuffersPerChannel,
            int floatingBuffersPerGate,
            int sortShuffleMinParallelism,
            int sortShuffleMinBuffers,
            boolean enableTieredStoreForHybridShuffle,
            int tieredStorageTotalExclusiveBuffers,
            int numSubpartitions) {

        Pair<Integer, Integer> minAndMax =
                getMinMaxNetworkBuffersPerResultPartition(
                        configuredNetworkBuffersPerChannel,
                        floatingBuffersPerGate,
                        sortShuffleMinParallelism,
                        sortShuffleMinBuffers,
                        numSubpartitions,
                        enableTieredStoreForHybridShuffle,
                        tieredStorageTotalExclusiveBuffers,
                        type);

        // In order to avoid network buffer request timeout (see FLINK-12852), we announce
        // network buffer requirement by below:
        // 1. For canBePipelined shuffle, the floating buffers may not be returned in time due to
        // back pressure so we need to include all the floating buffers in the announcement, i.e. we
        // should take the max value;
        // 2. For blocking shuffle, it is back pressure free and floating buffers can be recycled
        // in time, so that the minimum required buffers would be enough.
        int ret = type.canBePipelinedConsumed() ? minAndMax.getRight() : minAndMax.getLeft();

        if (ret == Integer.MAX_VALUE) {
            // Should never reach this branch. Result partition will allocate an unbounded
            // buffer pool only when type is ResultPartitionType.PIPELINED. But fine-grained
            // resource management is disabled in such case.
            throw new IllegalArgumentException(
                    "Illegal to announce network memory requirement as Integer.MAX_VALUE, partition type: "
                            + type);
        }
        return ret;
    }

    /** Private default constructor to avoid being instantiated. */
    private NettyShuffleUtils() {}
}
