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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream;

import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TierStorage;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TierStorageFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageWriterFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.UpstreamTieredStoreMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.file.PartitionFileManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.local.disk.UpstreamDiskTierStorageFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.local.memory.UpstreamMemoryTierStorageFactory;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;

/** {@link UpstreamTieredStorageFactory} is used to get storage in upstream. */
public class UpstreamTieredStorageFactory {

    private final boolean isBroadcast;
    private final PartitionFileManager partitionFileManager;
    private final UpstreamTieredStoreMemoryManager storeMemoryManager;
    private final TieredStorageWriterFactory tieredStorageWriterFactory;

    private final int[] tierIndexes;
    private final int numSubpartitions;
    private final float minReservedDiskSpaceFraction;

    private final String dataFileBasePath;

    private final TierStorage[] tierStorages;

    private final ResultPartitionID resultPartitionID;

    public UpstreamTieredStorageFactory(
            int[] tierIndexes,
            ResultPartitionID resultPartitionID,
            int numSubpartitions,
            float minReservedDiskSpaceFraction,
            String dataFileBasePath,
            boolean isBroadcast,
            PartitionFileManager partitionFileManager,
            UpstreamTieredStoreMemoryManager storeMemoryManager,
            TieredStorageWriterFactory tieredStorageWriterFactory) {
        this.tierIndexes = tierIndexes;
        this.numSubpartitions = numSubpartitions;
        this.minReservedDiskSpaceFraction = minReservedDiskSpaceFraction;
        this.dataFileBasePath = dataFileBasePath;
        this.isBroadcast = isBroadcast;
        this.partitionFileManager = partitionFileManager;
        this.storeMemoryManager = storeMemoryManager;
        this.tieredStorageWriterFactory = tieredStorageWriterFactory;
        this.tierStorages = new TierStorage[tierIndexes.length];
        this.resultPartitionID = resultPartitionID;
    }

    public void setup() {
        try {
            for (int i = 0; i < tierIndexes.length; i++) {
                tierStorages[i] = createTierStorage(tierIndexes[i]);
            }
        } catch (IOException e) {
            ExceptionUtils.rethrow(e);
        }
    }

    public TierStorage[] getTierStorages() {
        return tierStorages;
    }

    private TierStorage createTierStorage(int tierIndex) throws IOException {
        TierStorageFactory tierStorageFactory;
        switch (tierIndex) {
            case 0:
                tierStorageFactory = getMemoryTierStorageFactory();
                break;
            case 1:
                tierStorageFactory = getDiskTierStorageFactory();
                break;
            default:
                throw new IllegalArgumentException("Illegal tier type " + tierIndex);
        }
        TierStorage tierStorage = tierStorageFactory.createTierStorage();
        tierStorage.setup();
        return tierStorage;
    }

    private TierStorageFactory getMemoryTierStorageFactory() {
        return new UpstreamMemoryTierStorageFactory(
                numSubpartitions, storeMemoryManager, isBroadcast, tieredStorageWriterFactory);
    }

    private TierStorageFactory getDiskTierStorageFactory() {
        return new UpstreamDiskTierStorageFactory(
                numSubpartitions,
                resultPartitionID,
                dataFileBasePath,
                minReservedDiskSpaceFraction,
                isBroadcast,
                partitionFileManager,
                tieredStorageWriterFactory);
    }
}
