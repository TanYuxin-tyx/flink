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
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageIdMappingUtils;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageResource;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageResourceRegistry;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierMasterAgent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageUtils.deletePath;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageUtils.generateToReleaseJobPath;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageUtils.generateToReleasePartitionPath;

public class RemoteTierMasterAgent implements TierMasterAgent {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteTierMasterAgent.class);

    private final TieredStorageResourceRegistry resourceRegistry;

    private final String remoteStorageBaseHomePath;

    private final Map<ResultPartitionID, JobID> resultPartitionIdToJobIds;

    public RemoteTierMasterAgent(
            TieredStorageResourceRegistry resourceRegistry, String remoteStorageBaseHomePath) {
        this.resourceRegistry = resourceRegistry;
        this.remoteStorageBaseHomePath = remoteStorageBaseHomePath;
        this.resultPartitionIdToJobIds = new HashMap<>();
    }

    @Override
    public void addPartition(JobID jobID, ResultPartitionID resultPartitionID) {
        resourceRegistry.registerResource(
                TieredStorageIdMappingUtils.convertId(resultPartitionID),
                createResourceOfReleasePartitionFiles(jobID, resultPartitionID));
        resultPartitionIdToJobIds.putIfAbsent(resultPartitionID, jobID);
    }

    @Override
    public void releasePartition(ResultPartitionID resultPartitionID) {
        JobID jobID = resultPartitionIdToJobIds.remove(resultPartitionID);
        if (jobID != null) {
            resourceRegistry.clearResourceFor(
                    TieredStorageIdMappingUtils.convertId(resultPartitionID));
        }
    }

    @Override
    public void release(JobID jobID) {
        String toReleasePath = generateToReleaseJobPath(jobID, remoteStorageBaseHomePath);
        deletePathQuietly(toReleasePath);
    }

    private TieredStorageResource createResourceOfReleasePartitionFiles(
            JobID jobID, ResultPartitionID resultPartitionID) {
        return () -> {
            String toReleasePath =
                    generateToReleasePartitionPath(
                            jobID, resultPartitionID, remoteStorageBaseHomePath);
            deletePathQuietly(toReleasePath);
        };
    }

    private static void deletePathQuietly(String toReleasePath) {
        if (toReleasePath == null) {
            return;
        }

        try {
            deletePath(new Path(toReleasePath));
        } catch (IOException e) {
            LOG.error("Failed to delete files.", e);
        }
    }
}
