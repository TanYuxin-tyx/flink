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
import org.apache.flink.core.memory.MemorySegmentProvider;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.downstream.TierReaderFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.downstream.TierReaderFactoryImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.CacheFlushManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.UpstreamTieredStoreMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.file.PartitionFileManager;
import org.apache.flink.util.ExceptionUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

public class TieredStoreShuffleEnvironment {

    private final JobID jobID;

    private final String baseRemoteStoragePath;

    public TieredStoreShuffleEnvironment(JobID jobID, String baseRemoteStoragePath) {
        this.jobID = jobID;
        this.baseRemoteStoragePath = baseRemoteStoragePath;
    }

    public UpstreamTieredStorageFactory createUpstreamTieredStorageFactory(
            TierType[] tierTypes,
            ResultPartitionID resultPartitionID,
            int numSubpartitions,
            int bufferSize,
            float minReservedDiskSpaceFraction,
            String dataFileBasePath,
            boolean isBroadcast,
            @Nullable BufferCompressor bufferCompressor,
            PartitionFileManager partitionFileManager,
            UpstreamTieredStoreMemoryManager storeMemoryManager,
            CacheFlushManager cacheFlushManager) {
        UpstreamTieredStorageFactory tierStorageFactory = null;
        try {
            tierStorageFactory =
                    new UpstreamTieredStorageFactory(
                            tierTypes,
                            resultPartitionID,
                            numSubpartitions,
                            bufferSize,
                            minReservedDiskSpaceFraction,
                            dataFileBasePath,
                            isBroadcast,
                            bufferCompressor,
                            partitionFileManager,
                            storeMemoryManager,
                            cacheFlushManager);
            tierStorageFactory.setup();
        } catch (IOException e) {
            ExceptionUtils.rethrow(e);
        }
        return tierStorageFactory;
    }

    public RemoteTieredStorageFactory createRemoteTieredStorageFactory(
            TierType[] tierTypes,
            ResultPartitionID resultPartitionID,
            int numSubpartitions,
            int bufferSize,
            boolean isBroadcast,
            @Nullable BufferCompressor bufferCompressor,
            PartitionFileManager partitionFileManager,
            UpstreamTieredStoreMemoryManager storeMemoryManager,
            CacheFlushManager cacheFlushManager) {
        RemoteTieredStorageFactory tierStorageFactory = null;
        try {
            tierStorageFactory =
                    new RemoteTieredStorageFactory(
                            tierTypes,
                            resultPartitionID,
                            numSubpartitions,
                            bufferSize,
                            baseRemoteStoragePath,
                            isBroadcast,
                            bufferCompressor,
                            partitionFileManager,
                            storeMemoryManager,
                            cacheFlushManager);
            tierStorageFactory.setup();
        } catch (IOException e) {
            ExceptionUtils.rethrow(e);
        }
        return tierStorageFactory;
    }

    public TierReaderFactory createStorageTierReaderFactory(
            List<ResultPartitionID> resultPartitionIDs,
            MemorySegmentProvider memorySegmentProvider,
            List<Integer> subpartitionIndexes) {
        return new TierReaderFactoryImpl(
                jobID,
                resultPartitionIDs,
                memorySegmentProvider,
                subpartitionIndexes,
                baseRemoteStoragePath);
    }
}
