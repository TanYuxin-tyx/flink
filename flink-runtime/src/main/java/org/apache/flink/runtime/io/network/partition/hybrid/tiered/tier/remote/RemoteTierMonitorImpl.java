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
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageIdMappingUtils;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FatalExitExceptionHandler;

import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageUtils.generateSegmentFinishPath;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageUtils.getBaseSubpartitionPath;

/** Default implementation of {@link RemoteTierMonitor}. */
public class RemoteTierMonitorImpl implements RemoteTierMonitor {

    private final JobID jobID;

    private final InputStream[] inputStreams;

    private final ScheduledExecutorService monitorExecutor =
            Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder()
                            .setNameFormat("tiered store remote tier monitor")
                            .build());

    private final BiConsumer<Integer, Boolean> queueChannelCallBack;

    private final String baseRemoteStoragePath;

    private final boolean isUpstreamBroadcast;

    private final int[] requiredSegmentIds;

    private final int[] scanningSegmentIds;

    private final int[] readingSegmentIds;

    private final List<Tuple2<TieredStoragePartitionId, TieredStorageSubpartitionId>>
            partitionIdAndSubpartitionIds;

    private FileSystem remoteFileSystem;

    public RemoteTierMonitorImpl(
            List<Tuple2<TieredStoragePartitionId, TieredStorageSubpartitionId>>
                    partitionIdAndSubpartitionIds,
            JobID jobID,
            String baseRemoteStoragePath,
            boolean isUpstreamBroadcast,
            BiConsumer<Integer, Boolean> queueChannelCallBack) {
        this.requiredSegmentIds = new int[partitionIdAndSubpartitionIds.size()];
        this.scanningSegmentIds = new int[partitionIdAndSubpartitionIds.size()];
        this.readingSegmentIds = new int[partitionIdAndSubpartitionIds.size()];
        Arrays.fill(readingSegmentIds, -1);
        this.inputStreams = new InputStream[partitionIdAndSubpartitionIds.size()];
        this.jobID = jobID;
        this.baseRemoteStoragePath = baseRemoteStoragePath;
        this.isUpstreamBroadcast = isUpstreamBroadcast;
        this.queueChannelCallBack = queueChannelCallBack;
        this.partitionIdAndSubpartitionIds = partitionIdAndSubpartitionIds;
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
        try {
            for (int subpartitionId = 0;
                    subpartitionId < partitionIdAndSubpartitionIds.size();
                    subpartitionId++) {
                boolean isEnqueue = false;
                synchronized (this) {
                    int scanningSegmentId = scanningSegmentIds[subpartitionId];
                    int requiredSegmentId = requiredSegmentIds[subpartitionId];
                    if (scanningSegmentId <= requiredSegmentId
                            && isExist(subpartitionId, scanningSegmentId)) {
                        scanningSegmentIds[subpartitionId] = scanningSegmentId + 1;
                        isEnqueue = true;
                    }
                }
                if (isEnqueue) {
                    queueChannelCallBack.accept(subpartitionId, false);
                }
            }
        } catch (Exception e) {
            FatalExitExceptionHandler.INSTANCE.uncaughtException(Thread.currentThread(), e);
        }
    }

    @Override
    public boolean isExist(int subpartitionId, int segmentId) {
        String baseSubpartitionPath =
                getBaseSubpartitionPath(
                        jobID,
                        TieredStorageIdMappingUtils.convertId(
                                partitionIdAndSubpartitionIds.get(subpartitionId).f0),
                        partitionIdAndSubpartitionIds.get(subpartitionId).f1.getSubpartitionId(),
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

    @Override
    public void getSegmentFileInputStream(int subpartitionId, int segmentId) {
        synchronized (this) {
            if (readingSegmentIds[subpartitionId] != segmentId) {
                readingSegmentIds[subpartitionId] = segmentId;
            }
        }
    }

    @Override
    public void updateRequiredSegmentId(int subpartitionId, int segmentId) {
        synchronized (this) {
            requiredSegmentIds[subpartitionId] = segmentId;
        }
    }

    @Override
    public void close() {
        monitorExecutor.shutdownNow();
    }
}
