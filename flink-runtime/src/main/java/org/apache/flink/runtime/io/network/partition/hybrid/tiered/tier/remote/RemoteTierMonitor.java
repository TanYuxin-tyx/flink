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
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;

import java.util.List;
import java.util.function.BiConsumer;

/**
 * The {@link RemoteTierMonitor} is the monitor to scan the existing status of shuffle data stored
 * in Remote Tier.
 */
public interface RemoteTierMonitor extends Runnable {

    /** Start the remote tier monitor. */
    void start();

    /**
     * Return the existence status of the segment file.
     *
     * @param subpartitionId subpartition id that indicates the id of subpartition.
     * @param segmentId segment id that indicates the id of segment.
     * @return whether the segment file exists.
     */
    boolean isExist(int subpartitionId, int segmentId);

    /**
     * Update the required segment id.
     *
     * @param subpartitionId subpartition id that indicates the id of subpartition.
     * @param segmentId segment id that indicates the id of segment.
     */
    void updateRequiredSegmentId(int subpartitionId, int segmentId);
    /** Close the remote tier monitor */
    void close();

    /** Factory to create {@link RemoteTierMonitor}. */
    class Factory {

        static RemoteTierMonitor createRemoteTierMonitor(
                List<Tuple2<TieredStoragePartitionId, TieredStorageSubpartitionId>>
                        partitionIdAndSubpartitionIds,
                JobID jobID,
                String baseRemoteStoragePath,
                BiConsumer<Integer, Boolean> queueChannelCallBack,
                boolean isUpstreamBroadcastOnly) {
            return new RemoteTierMonitorImpl(partitionIdAndSubpartitionIds,
                    jobID,
                    baseRemoteStoragePath,
                    isUpstreamBroadcastOnly,
                    queueChannelCallBack);
        }
    }
}
