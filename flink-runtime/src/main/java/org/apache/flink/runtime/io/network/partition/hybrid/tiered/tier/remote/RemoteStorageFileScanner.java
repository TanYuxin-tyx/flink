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

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;

/**
 * The {@link RemoteStorageFileScanner} is the monitor to scan the existing status of shuffle data
 * stored in remote storage. It will be invoked by {@link RemoteTierConsumerAgent} to register the
 * required segment id and trigger the reading of {@link RemoteTierConsumerAgent}, and it also
 * provides a method to read buffer from remote storage.
 */
public interface RemoteStorageFileScanner extends Runnable {

    /** Start the {@link RemoteStorageFileScanner}. */
    void start();

    /**
     * Register a segment id to the {@link RemoteStorageFileScanner}. If the scanner discovers the
     * segment file exists, it will trigger the next round of reading.
     *
     * @param partitionId partition id indicates the id of partition.
     * @param subpartitionId subpartition id indicates the id of subpartition.
     * @param segmentId segment id indicates the id of segment.
     */
    void registerSegmentId(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId);

    /**
     * Read buffer from remote storage.
     *
     * @param partitionId partition id indicates the id of partition.
     * @param subpartitionId subpartition id indicates the id of subpartition.
     * @param segmentId segment id indicates the id of segment.
     * @return buffer.
     */
    Buffer readBuffer(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId);

    /** Close the {@link RemoteStorageFileScanner}. */
    void close();
}
