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

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.MemorySegmentProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.downstream.TierReaderFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.downstream.TierReaderFactoryImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.UpstreamTieredStoreMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.file.PartitionFileManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.tier.local.UpstreamTierStorageReleaser;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.tier.remote.RemoteTierStorageReleaser;

import java.util.List;

import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.TieredStoreUtils.generateToReleasePath;

public class TieredStoreShuffleEnvironment {

    public UpstreamTieredStorageFactory createUpstreamTieredStorageFactory(
            TierType[] tierTypes,
            ResultPartitionID resultPartitionID,
            int numSubpartitions,
            float minReservedDiskSpaceFraction,
            String dataFileBasePath,
            boolean isBroadcast,
            PartitionFileManager partitionFileManager,
            UpstreamTieredStoreMemoryManager storeMemoryManager,
            TieredStorageWriterFactory tieredStorageWriterFactory) {
        UpstreamTieredStorageFactory tierStorageFactory =
                new UpstreamTieredStorageFactory(
                        tierTypes,
                        resultPartitionID,
                        numSubpartitions,
                        minReservedDiskSpaceFraction,
                        dataFileBasePath,
                        isBroadcast,
                        partitionFileManager,
                        storeMemoryManager,
                        tieredStorageWriterFactory);
        tierStorageFactory.setup();
        return tierStorageFactory;
    }

    public RemoteTieredStorageFactory createRemoteTieredStorageFactory(
            TierType[] tierTypes, TieredStorageWriterFactory tieredStorageWriterFactory) {
        RemoteTieredStorageFactory tierStorageFactory =
                new RemoteTieredStorageFactory(tierTypes, tieredStorageWriterFactory);
        tierStorageFactory.setup();
        return tierStorageFactory;
    }

    public TierReaderFactory createStorageTierReaderFactory(
            JobID jobID,
            List<ResultPartitionID> resultPartitionIDs,
            MemorySegmentProvider memorySegmentProvider,
            List<Integer> subpartitionIndexes,
            String baseRemoteStoragePath) {
        return new TierReaderFactoryImpl(
                jobID,
                resultPartitionIDs,
                memorySegmentProvider,
                subpartitionIndexes,
                baseRemoteStoragePath);
    }

    public TierStorageReleaser[] createStorageTierReleasers(
            JobID jobID, String baseRemoteStoragePath) {
        Path toReleasePath = generateToReleasePath(jobID, baseRemoteStoragePath);
        TierStorageReleaser[] tierStorageReleasers = new TierStorageReleaser[2];
        tierStorageReleasers[0] = new UpstreamTierStorageReleaser();
        tierStorageReleasers[1] = new RemoteTierStorageReleaser(toReleasePath);
        return tierStorageReleasers;
    }
}
