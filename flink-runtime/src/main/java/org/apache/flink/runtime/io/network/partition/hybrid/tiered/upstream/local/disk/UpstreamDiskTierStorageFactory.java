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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.local.disk;

import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TierStorage;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TierStorageFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageWriterFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.file.PartitionFileManager;

public class UpstreamDiskTierStorageFactory implements TierStorageFactory {

    private final int numSubpartitions;

    private final ResultPartitionID resultPartitionID;

    private final boolean isBroadcast;

    private final float minReservedDiskSpaceFraction;

    private final String dataFileBasePath;

    private final PartitionFileManager partitionFileManager;

    private final TieredStorageWriterFactory tieredStorageWriterFactory;

    public UpstreamDiskTierStorageFactory(
            int numSubpartitions,
            ResultPartitionID resultPartitionID,
            String dataFileBasePath,
            float minReservedDiskSpaceFraction,
            boolean isBroadcast,
            PartitionFileManager partitionFileManager,
            TieredStorageWriterFactory tieredStorageWriterFactory) {
        this.numSubpartitions = numSubpartitions;
        this.resultPartitionID = resultPartitionID;
        this.isBroadcast = isBroadcast;
        this.dataFileBasePath = dataFileBasePath;
        this.minReservedDiskSpaceFraction = minReservedDiskSpaceFraction;
        this.partitionFileManager = partitionFileManager;
        this.tieredStorageWriterFactory = tieredStorageWriterFactory;
    }

    @Override
    public TierStorage createTierStorage() {
        return new DiskTierStorage(
                numSubpartitions,
                resultPartitionID,
                dataFileBasePath,
                minReservedDiskSpaceFraction,
                isBroadcast,
                partitionFileManager,
                tieredStorageWriterFactory);
    }
}
