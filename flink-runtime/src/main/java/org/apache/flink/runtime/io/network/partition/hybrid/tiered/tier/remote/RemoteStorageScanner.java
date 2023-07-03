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
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate.AvailabilityAndPriorityRetriever;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageConsumerSpec;
import org.apache.flink.util.ExceptionUtils;

import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageUtils.getSegmentFinishPath;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageUtils.getSubpartitionPath;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The {@link RemoteStorageScanner} is introduced to notify asynchronously for file reading on
 * remote storage. Asynchronous notifications will prevent {@link RemoteTierConsumerAgent} from
 * repeatedly attempting to read remote files and reduce CPU consumption.
 *
 * <p>It will be invoked by {@link RemoteTierConsumerAgent} to register the required segment id and
 * monitor the existence status of related segment files. If the segment file is found, it will
 * trigger the reading process of the file.
 */
public class RemoteStorageScanner implements Runnable {

    /** Executor to scan the existence status of segment files on remote storage. */
    private final ScheduledExecutorService scannerExecutor =
            Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder()
                            .setNameFormat("remote storage file scanner")
                            .build());

    /**
     * Tuple3 containing partition id and subpartition id and segment id. The tuple3 is stored in
     * queue and indicates the required segment file.
     */
    private final Queue<Tuple3<TieredStoragePartitionId, TieredStorageSubpartitionId, Integer>>
            requiredSegmentIds;

    private final String baseRemoteStoragePath;

    private FileSystem remoteFileSystem;

    private AvailabilityAndPriorityRetriever retriever;

    public RemoteStorageScanner(
            List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs,
            String baseRemoteStoragePath) {
        this.baseRemoteStoragePath = baseRemoteStoragePath;
        this.requiredSegmentIds = new LinkedBlockingDeque<>();
        try {
            this.remoteFileSystem = new Path(baseRemoteStoragePath).getFileSystem();
        } catch (IOException e) {
            ExceptionUtils.rethrow(
                    e, "Failed to initialize file system on the path: " + baseRemoteStoragePath);
        }
    }

    /** Start the executor. */
    public void start() {
        scannerExecutor.scheduleAtFixedRate(this, 0, 10, TimeUnit.MILLISECONDS);
    }

    /**
     * Register a segment id to the {@link RemoteStorageScanner}. If the scanner discovers the
     * segment file exists, it will trigger the reading process of the segment file.
     *
     * @param partitionId is the id of partition.
     * @param subpartitionId is the id of subpartition.
     * @param segmentId is the id of segment.
     */
    public void registerSegmentId(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId) {
        requiredSegmentIds.add(Tuple3.of(partitionId, subpartitionId, segmentId));
    }

    /** Close the executor. */
    public void close() {
        scannerExecutor.shutdownNow();
    }

    /** Iterate the registered segment ids and check related file status. */
    @Override
    public void run() {
        int segmentFileNumber = requiredSegmentIds.size();
        while (segmentFileNumber-- > 0) {
            Tuple3<TieredStoragePartitionId, TieredStorageSubpartitionId, Integer>
                    partitionIdAndSubpartitionIdAndSegmentId =
                            checkNotNull(requiredSegmentIds.poll());
            if (checkFileExist(
                    partitionIdAndSubpartitionIdAndSegmentId.f0,
                    partitionIdAndSubpartitionIdAndSegmentId.f1,
                    partitionIdAndSubpartitionIdAndSegmentId.f2)) {
                retriever.retrieveAvailableAndPriority(
                        partitionIdAndSubpartitionIdAndSegmentId.f0,
                        partitionIdAndSubpartitionIdAndSegmentId.f1,
                        false,
                        null);
            } else {
                requiredSegmentIds.add(partitionIdAndSubpartitionIdAndSegmentId);
            }
        }
    }

    public void registerAvailabilityAndPriorityRetriever(
            AvailabilityAndPriorityRetriever retriever) {
        this.retriever = retriever;
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    private boolean checkFileExist(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId) {
        String baseSubpartitionPath =
                getSubpartitionPath(
                        baseRemoteStoragePath, partitionId, subpartitionId.getSubpartitionId());
        Path currentSegmentFinishPath = getSegmentFinishPath(baseSubpartitionPath, segmentId);
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
