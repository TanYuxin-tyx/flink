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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.local.disk;

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStoragePartitionIdAndSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.NettyPayload;

import java.util.List;

/**
 * This interface is used by operate. Spilling decision may be made and handled inside these
 * operations.
 */
public interface DiskCacheManagerOperation {
    /**
     * Get the number of downstream consumers.
     *
     * @return Number of subpartitions.
     */
    int getNumSubpartitions();

    /** Get all buffers from the subpartition. */
    List<NettyPayload> getBuffersInOrder(int subpartitionId);

    /**
     * This method is called when consumer is decided to released.
     *
     * @param subpartitionId the subpartition's identifier that this consumer belongs to.
     * @param nettyServiceWriterId the consumer's identifier which decided to be released.
     */
    void onConsumerReleased(
            int subpartitionId, TieredStoragePartitionIdAndSubpartitionId nettyServiceWriterId);
}
