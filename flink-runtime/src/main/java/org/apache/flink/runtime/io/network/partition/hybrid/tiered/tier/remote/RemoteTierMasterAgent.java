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

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageIdMappingUtils;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageResource;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageResourceRegistry;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierMasterAgent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageUtils.deletePathQuietly;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageUtils.generateToReleasePartitionPath;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageUtils.removePartitionFiles;

public class RemoteTierMasterAgent implements TierMasterAgent {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteTierMasterAgent.class);

    private final TieredStorageResourceRegistry resourceRegistry;

    private final String remoteStorageBaseHomePath;

    RemoteTierMasterAgent(
            TieredStorageResourceRegistry resourceRegistry, String remoteStorageBaseHomePath) {
        this.resourceRegistry = resourceRegistry;
        this.remoteStorageBaseHomePath = remoteStorageBaseHomePath;
    }

    @Override
    public void addPartition(ResultPartitionID resultPartitionID) {
        resourceRegistry.registerResource(
                TieredStorageIdMappingUtils.convertId(resultPartitionID),
                createResourceOfReleasePartitionFiles(resultPartitionID));
    }

    @Override
    public void releasePartition(ResultPartitionID resultPartitionID) {
        try {
            removePartitionFiles(new Path(remoteStorageBaseHomePath), resultPartitionID);
        } catch (IOException e) {
            LOG.error("Failed to release the partition files for {}", resultPartitionID);
        }

        resourceRegistry.clearResourceFor(TieredStorageIdMappingUtils.convertId(resultPartitionID));
    }

    @Override
    public void release(String pathToRelease) {
        deletePathQuietly(pathToRelease);
    }

    private TieredStorageResource createResourceOfReleasePartitionFiles(
            ResultPartitionID resultPartitionID) {
        return () -> {
            String toReleasePath =
                    generateToReleasePartitionPath(resultPartitionID, remoteStorageBaseHomePath);
            deletePathQuietly(toReleasePath);
        };
    }
}
