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
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate.AvailabilityAndPriorityRetriever;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.util.ExceptionUtils;

import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.SegmentPartitionFile.getSegmentFinishDirPath;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

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

    /** The initial scan interval is 500ms. */
    private static final int INITIAL_SCAN_INTERVAL = 500;

    /** The max scan interval is 10000ms. */
    private static final int MAX_SCAN_INTERVAL = 10_000;

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

    /**
     * The key is partition id and subpartition id, the value is max id of written segment files in
     * the subpartition.
     */
    private final Map<TieredStoragePartitionId, Map<TieredStorageSubpartitionId, Integer>>
            scannedMaxSegmentIds;

    private final String baseRemoteStoragePath;

    private final ScanStrategy scanStrategy;

    private FileSystem remoteFileSystem;

    private AvailabilityAndPriorityRetriever retriever;

    private int attemptNumber = 0;

    public RemoteStorageScanner(String baseRemoteStoragePath) {
        this.baseRemoteStoragePath = baseRemoteStoragePath;
        this.requiredSegmentIds = new LinkedBlockingDeque<>();
        this.scannedMaxSegmentIds = new HashMap<>();
        this.scanStrategy = new ScanStrategy(INITIAL_SCAN_INTERVAL, MAX_SCAN_INTERVAL);
        try {
            this.remoteFileSystem = new Path(baseRemoteStoragePath).getFileSystem();
        } catch (IOException e) {
            ExceptionUtils.rethrow(
                    e, "Failed to initialize file system on the path: " + baseRemoteStoragePath);
        }
    }

    /** Start the executor. */
    public void start() {
        scannerExecutor.schedule(
                this, scanStrategy.getInterval(attemptNumber), TimeUnit.MILLISECONDS);
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
        boolean scanned = false;
        while (segmentFileNumber-- > 0) {
            Tuple3<TieredStoragePartitionId, TieredStorageSubpartitionId, Integer> ids =
                    checkNotNull(requiredSegmentIds.poll());
            TieredStoragePartitionId partitionId = ids.f0;
            TieredStorageSubpartitionId subpartitionId = ids.f1;
            int requiredSegmentId = ids.f2;
            int maxSegmentId =
                    scannedMaxSegmentIds
                            .computeIfAbsent(partitionId, ignore -> new HashMap<>())
                            .getOrDefault(subpartitionId, -1);
            if (maxSegmentId >= requiredSegmentId) {
                scanned = true;
                retriever.retrieveAvailableAndPriority(partitionId, subpartitionId, false, null);
            } else {
                scanMaxSegmentId(partitionId, subpartitionId);
                requiredSegmentIds.add(ids);
            }
        }
        attemptNumber = scanned ? 0 : attemptNumber + 1;
        scannerExecutor.schedule(
                this, scanStrategy.getInterval(attemptNumber), TimeUnit.MILLISECONDS);
    }

    public void registerAvailabilityAndPriorityRetriever(
            AvailabilityAndPriorityRetriever retriever) {
        this.retriever = retriever;
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    /**
     * Scan the max segment id of segment files for the specific partition and subpartition. The max
     * segment id can be obtained from a file named by max segment id.
     *
     * @param partitionId the partition id.
     * @param subpartitionId the subpartition id.
     */
    private void scanMaxSegmentId(
            TieredStoragePartitionId partitionId, TieredStorageSubpartitionId subpartitionId) {
        Path segmentFinishDir =
                getSegmentFinishDirPath(
                        baseRemoteStoragePath, partitionId, subpartitionId.getSubpartitionId());
        FileStatus[] fileStatuses;
        try {
            if (!remoteFileSystem.exists(segmentFinishDir)) {
                return;
            }
            fileStatuses = remoteFileSystem.listStatus(segmentFinishDir);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to list the segment finish file. " + segmentFinishDir, e);
        }
        if (fileStatuses.length == 0) {
            return;
        }
        checkState(fileStatuses.length == 1);
        scannedMaxSegmentIds
                .computeIfAbsent(partitionId, ignore -> new HashMap<>())
                .put(subpartitionId, Integer.parseInt(fileStatuses[0].getPath().getName()));
    }

    /**
     * The strategy is used to decide the scan interval of {@link RemoteStorageScanner}. The
     * interval will be updated exponentially and restricted by max value.
     */
    private static class ScanStrategy {

        private final int initialScanInterval;

        private final int maxScanInterval;

        public ScanStrategy(int initialScanInterval, int maxScanInterval) {
            checkArgument(
                    initialScanInterval > 0,
                    "initialScanInterval must be positive, was %s",
                    initialScanInterval);
            checkArgument(
                    maxScanInterval > 0,
                    "maxScanInterval must be positive, was %s",
                    maxScanInterval);
            checkArgument(
                    initialScanInterval <= maxScanInterval,
                    "initialScanInterval must be lower than or equal to maxScanInterval",
                    maxScanInterval);
            this.initialScanInterval = initialScanInterval;
            this.maxScanInterval = maxScanInterval;
        }

        public long getInterval(int attempt) {
            checkArgument(attempt >= 0, "attempt must not be negative (%s)", attempt);
            final long interval = initialScanInterval * Math.round(Math.pow(2, attempt));
            return interval >= 0 && interval < maxScanInterval ? interval : maxScanInterval;
        }
    }
}
