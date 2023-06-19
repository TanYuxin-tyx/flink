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

import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageUtils.generateNewSegmentPath;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageUtils.generateSegmentFinishPath;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageUtils.getBaseSubpartitionPath;

/** Default implementation of {@link RemoteTierMonitor}. */
public class RemoteTierMonitorImpl implements RemoteTierMonitor {

    private final JobID jobID;

    private final List<TieredStoragePartitionId> resultPartitionIDs;

    private final InputStream[] inputStreams;

    private final List<TieredStorageSubpartitionId> subpartitionIds;

    private final ScheduledExecutorService monitorExecutor =
            Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder()
                            .setNameFormat("tiered store remote tier monitor")
                            .build());

    private final BiConsumer<Integer, Boolean> queueChannelCallBack;

    private final String baseRemoteStoragePath;

    private final int numSubpartitions;

    private final boolean isUpstreamBroadcast;

    private final int[] requiredSegmentIds;

    private final int[] scanningSegmentIds;

    private final int[] readingSegmentIds;

    private FileSystem remoteFileSystem;

    public RemoteTierMonitorImpl(
            JobID jobID,
            List<TieredStoragePartitionId> resultPartitionIDs,
            String baseRemoteStoragePath,
            List<TieredStorageSubpartitionId> subpartitionIdes,
            int numSubpartitions,
            boolean isUpstreamBroadcast,
            BiConsumer<Integer, Boolean> queueChannelCallBack) {
        this.requiredSegmentIds = new int[subpartitionIdes.size()];
        this.scanningSegmentIds = new int[subpartitionIdes.size()];
        this.readingSegmentIds = new int[subpartitionIdes.size()];
        Arrays.fill(readingSegmentIds, -1);
        this.inputStreams = new InputStream[subpartitionIdes.size()];
        this.subpartitionIds = subpartitionIdes;
        this.jobID = jobID;
        this.resultPartitionIDs = resultPartitionIDs;
        this.baseRemoteStoragePath = baseRemoteStoragePath;
        this.isUpstreamBroadcast = isUpstreamBroadcast;
        this.numSubpartitions = numSubpartitions;
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
        try {
            for (int subpartitionId = 0; subpartitionId < numSubpartitions; subpartitionId++) {
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
                        TieredStorageIdMappingUtils.convertId(resultPartitionIDs.get(subpartitionId)),
                        subpartitionIds.get(subpartitionId).getSubpartitionId(),
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
    public InputStream getSegmentFileInputStream(int subpartitionId, int segmentId) {
        synchronized (this) {
            InputStream requiredInputStream = inputStreams[subpartitionId];
            if (requiredInputStream == null || readingSegmentIds[subpartitionId] != segmentId) {
                String baseSubpartitionPath =
                        getBaseSubpartitionPath(
                                jobID,
                                TieredStorageIdMappingUtils.convertId(resultPartitionIDs.get(subpartitionId)),
                                subpartitionIds.get(subpartitionId).getSubpartitionId(),
                                baseRemoteStoragePath,
                                isUpstreamBroadcast);
                Path currentSegmentPath = generateNewSegmentPath(baseSubpartitionPath, segmentId);
                InputStream inputStream = null;
                try {
                    inputStream = remoteFileSystem.open(currentSegmentPath);
                } catch (IOException e) {
                    ExceptionUtils.rethrow(
                            e, "Failed to open the segment path: " + currentSegmentPath);
                }
                inputStreams[subpartitionId] = inputStream;
                readingSegmentIds[subpartitionId] = segmentId;
                return inputStream;
            } else {
                return inputStreams[subpartitionId];
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
