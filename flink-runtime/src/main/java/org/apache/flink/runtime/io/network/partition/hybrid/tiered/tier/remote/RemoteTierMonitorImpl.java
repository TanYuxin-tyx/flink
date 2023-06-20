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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageIdMappingUtils;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.util.ExceptionUtils;

import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageUtils.generateSegmentFinishPath;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageUtils.getBaseSubpartitionPath;

/** Default implementation of {@link RemoteTierMonitor}. */
public class RemoteTierMonitorImpl implements RemoteTierMonitor {

    private final JobID jobID;

    private final ScheduledExecutorService monitorExecutor =
            Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder()
                            .setNameFormat("tiered store remote tier monitor")
                            .build());

    private final BiConsumer<Integer, Boolean> queueChannelCallBack;

    private final String baseRemoteStoragePath;

    private final boolean isUpstreamBroadcast;

    private final Map<TieredStoragePartitionId, Map<TieredStorageSubpartitionId, Integer>>
            requiredSegmentIds;

    private final Map<TieredStoragePartitionId, Map<TieredStorageSubpartitionId, Integer>>
            channelIndexes;

    private FileSystem remoteFileSystem;

    public RemoteTierMonitorImpl(
            List<Tuple2<TieredStoragePartitionId, TieredStorageSubpartitionId>>
                    partitionIdAndSubpartitionIds,
            JobID jobID,
            String baseRemoteStoragePath,
            boolean isUpstreamBroadcast,
            BiConsumer<Integer, Boolean> queueChannelCallBack) {

        this.channelIndexes = new HashMap<>();
        for (int index = 0; index < partitionIdAndSubpartitionIds.size(); index++) {
            Tuple2<TieredStoragePartitionId, TieredStorageSubpartitionId> ids =
                    partitionIdAndSubpartitionIds.get(index);
            channelIndexes.computeIfAbsent(ids.f0, ignore -> new HashMap<>()).put(ids.f1, index);
        }
        this.requiredSegmentIds = new HashMap<>();
        this.jobID = jobID;
        this.baseRemoteStoragePath = baseRemoteStoragePath;
        this.isUpstreamBroadcast = isUpstreamBroadcast;
        this.queueChannelCallBack = queueChannelCallBack;
        try {
            this.remoteFileSystem = new Path(baseRemoteStoragePath).getFileSystem();
        } catch (IOException e) {
            ExceptionUtils.rethrow(
                    e, "Failed to initialize fileSystem on the path: " + baseRemoteStoragePath);
        }
    }

    @Override
    public void start() {
        monitorExecutor.scheduleAtFixedRate(this, 0, 10, TimeUnit.MILLISECONDS);
    }

    @Override
    public void run() {
        List<Integer> availableFiles = new ArrayList<>();
        List<Tuple3<TieredStoragePartitionId, TieredStorageSubpartitionId, Integer>> existFiles =
                new ArrayList<>();
        synchronized (this) {
            for (TieredStoragePartitionId partitionId : requiredSegmentIds.keySet()) {
                Map<TieredStorageSubpartitionId, Integer> subpartitionIdAndSegmentId =
                        requiredSegmentIds.get(partitionId);
                for (Map.Entry<TieredStorageSubpartitionId, Integer> entry :
                        subpartitionIdAndSegmentId.entrySet()) {
                    if (checkFileExist(partitionId, entry.getKey(), entry.getValue())) {
                        availableFiles.add(channelIndexes.get(partitionId).get(entry.getKey()));
                        existFiles.add(Tuple3.of(partitionId, entry.getKey(), entry.getValue()));
                    }
                }
            }
            for (Tuple3<TieredStoragePartitionId, TieredStorageSubpartitionId, Integer> existFile :
                    existFiles) {
                Map<TieredStorageSubpartitionId, Integer> subpartitionIdAndSegmentId =
                        requiredSegmentIds.get(existFile.f0);
                subpartitionIdAndSegmentId.remove(existFile.f1);
                if (subpartitionIdAndSegmentId.size() == 0) {
                    requiredSegmentIds.remove(existFile.f0);
                }
            }
        }
        for (int index : availableFiles) {
            queueChannelCallBack.accept(index, false);
        }
    }

    @Override
    public void monitorSegmentFile(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId) {
        synchronized (this) {
            requiredSegmentIds
                    .computeIfAbsent(partitionId, ignore -> new HashMap<>())
                    .put(subpartitionId, segmentId);
        }
    }

    @Override
    public void close() {
        monitorExecutor.shutdownNow();
    }

    private boolean checkFileExist(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId) {
        String baseSubpartitionPath =
                getBaseSubpartitionPath(
                        jobID,
                        TieredStorageIdMappingUtils.convertId(partitionId),
                        subpartitionId.getSubpartitionId(),
                        baseRemoteStoragePath,
                        isUpstreamBroadcast);
        Path currentSegmentFinishPath = generateSegmentFinishPath(baseSubpartitionPath, segmentId);
        try {
            return remoteFileSystem.exists(currentSegmentFinishPath);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to check the existing state of segment path: "
                            + currentSegmentFinishPath,
                    e);
        }
    }
}
