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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.remote;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.ConsumerNettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.ProducerNettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManagerImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageResourceRegistry;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file.PartitionFileManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierConsumerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierMasterAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierProducerAgent;
import org.apache.flink.util.ExceptionUtils;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;

public class RemoteTierFactory implements TierFactory {

    @Override
    public TierMasterAgent createMasterAgent(
            TieredStorageResourceRegistry resourceRegistry,
            @Nullable String remoteStorageBaseHomePath) {
        return new RemoteTierMasterAgent(resourceRegistry, remoteStorageBaseHomePath);
    }

    @Override
    public TierProducerAgent createProducerAgent(
            int tierIndex,
            int numSubpartitions,
            ResultPartitionID resultPartitionID,
            String dataFileBasePath,
            float minReservedDiskSpaceFraction,
            boolean isBroadcastOnly,
            PartitionFileManager partitionFileManager,
            int networkBufferSize,
            TieredStorageMemoryManager storageMemoryManager,
            BufferCompressor bufferCompressor,
            ProducerNettyService nettyService) {
        return new RemoteTierProducerAgent(
                numSubpartitions, isBroadcastOnly, storageMemoryManager, partitionFileManager);
    }

    @Override
    public TierConsumerAgent createConsumerAgent(
            int numSubpartitions,
            int[] requiredSegmentIds,
            List<Integer> subpartitionIds,
            JobID jobID,
            List<ResultPartitionID> resultPartitionIDs,
            NetworkBufferPool networkBufferPool,
            String baseRemoteStoragePath,
            ConsumerNettyService consumerNettyService,
            boolean isUpstreamBroadcastOnly) {

        TieredStorageMemoryManager storageMemoryManager = null;
        try {
            storageMemoryManager = new TieredStorageMemoryManagerImpl(1, false);
            storageMemoryManager.setup(
                    networkBufferPool.createBufferPool(1, 1), Collections.emptyList());
        } catch (Exception e) {
            ExceptionUtils.rethrow(e, "Failed to create TieredStorageMemoryManger.");
        }
        RemoteTierMonitor remoteTierMonitor =
                RemoteTierMonitor.Factory.createRemoteTierMonitor(
                        numSubpartitions,
                        subpartitionIds,
                        jobID,
                        resultPartitionIDs,
                        baseRemoteStoragePath,
                        consumerNettyService,
                        isUpstreamBroadcastOnly);
        return new RemoteTierConsumerAgent(
                numSubpartitions, storageMemoryManager, remoteTierMonitor, consumerNettyService);
    }
}
