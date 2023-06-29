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

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageIdMappingUtils;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageConsumerSpec;
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
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageUtils.generateSubpartitionPath;

/**
 * The {@link RemoteStorageFileScanner} is the monitor to scan the existing status of shuffle data
 * stored in remote storage. It will be invoked by {@link RemoteTierConsumerAgent} to register the
 * required segment id and trigger the reading of {@link RemoteTierConsumerAgent}, and it also
 * provides a method to read buffer from remote storage.
 */
public class RemoteStorageFileScanner implements Runnable {

    private final ScheduledExecutorService scannerExecutor =
            Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder()
                            .setNameFormat("remote storage file scanner")
                            .build());

    private final String baseRemoteStoragePath;

    private final Map<TieredStoragePartitionId, Map<TieredStorageSubpartitionId, Integer>>
            requiredSegmentIds;

    private final Map<TieredStoragePartitionId, Map<TieredStorageSubpartitionId, Integer>>
            channelIndexes;

    private FileSystem remoteFileSystem;

    private BiConsumer<Integer, Boolean> queueChannelCallBack;

    public RemoteStorageFileScanner(
            List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs,
            String baseRemoteStoragePath) {
        this.baseRemoteStoragePath = baseRemoteStoragePath;
        this.channelIndexes = new HashMap<>();
        for (int index = 0; index < tieredStorageConsumerSpecs.size(); index++) {
            TieredStorageConsumerSpec spec = tieredStorageConsumerSpecs.get(index);
            channelIndexes
                    .computeIfAbsent(spec.getPartitionId(), ignore -> new HashMap<>())
                    .put(spec.getSubpartitionId(), index);
        }
        this.requiredSegmentIds = new HashMap<>();
        try {
            this.remoteFileSystem = new Path(baseRemoteStoragePath).getFileSystem();
        } catch (IOException e) {
            ExceptionUtils.rethrow(
                    e, "Failed to initialize fileSystem on the path: " + baseRemoteStoragePath);
        }
    }

    public void start() {
        scannerExecutor.scheduleAtFixedRate(this, 0, 10, TimeUnit.MILLISECONDS);
    }

    @Override
    public void run() {
        List<Integer> availableInputChannelIndexes = new ArrayList<>();
        List<Tuple3<TieredStoragePartitionId, TieredStorageSubpartitionId, Integer>> existFiles =
                new ArrayList<>();
        synchronized (this) {
            for (TieredStoragePartitionId partitionId : requiredSegmentIds.keySet()) {
                Map<TieredStorageSubpartitionId, Integer> subpartitionIdAndSegmentId =
                        requiredSegmentIds.get(partitionId);
                for (Map.Entry<TieredStorageSubpartitionId, Integer> entry :
                        subpartitionIdAndSegmentId.entrySet()) {
                    if (checkFileExist(partitionId, entry.getKey(), entry.getValue())) {
                        availableInputChannelIndexes.add(
                                channelIndexes.get(partitionId).get(entry.getKey()));
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
        for (int channelIndex : availableInputChannelIndexes) {
            queueChannelCallBack.accept(channelIndex, false);
        }
    }

    /**
     * Register a segment id to the {@link RemoteStorageFileScanner}. If the scanner discovers the
     * segment file exists, it will trigger the next round of reading.
     *
     * @param partitionId partition id indicates the id of partition.
     * @param subpartitionId subpartition id indicates the id of subpartition.
     * @param segmentId segment id indicates the id of segment.
     */
    public void registerSegmentId(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId) {
        synchronized (this) {
            requiredSegmentIds
                    .computeIfAbsent(partitionId, ignore -> new HashMap<>())
                    .put(subpartitionId, segmentId);
        }
    }

    public void close() {
        scannerExecutor.shutdownNow();
    }

    public void setupQueueChannelCallBack(BiConsumer<Integer, Boolean> queueChannelCallBack) {
        this.queueChannelCallBack = queueChannelCallBack;
    }

    public void triggerSubpartitionReading(
            TieredStoragePartitionId partitionId, TieredStorageSubpartitionId subpartitionId) {
        queueChannelCallBack.accept(channelIndexes.get(partitionId).get(subpartitionId), false);
    }

    private boolean checkFileExist(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId) {
        String baseSubpartitionPath =
                generateSubpartitionPath(
                        baseRemoteStoragePath,
                        TieredStorageIdMappingUtils.convertId(partitionId),
                        subpartitionId.getSubpartitionId());
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
