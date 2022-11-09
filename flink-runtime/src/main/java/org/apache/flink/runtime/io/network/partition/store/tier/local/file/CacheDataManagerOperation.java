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

package org.apache.flink.runtime.io.network.partition.store.tier.local.file;

import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.partition.store.common.BufferIndexAndChannel;
import org.apache.flink.runtime.io.network.partition.store.common.ConsumerId;

import java.util.Collection;

/**
 * This interface is used by {@link SubpartitionConsumerCacheDataManager} to operate {@link
 * CacheDataManager}. Spilling decision may be made and handled inside these operations.
 */
public interface CacheDataManagerOperation {
    /**
     * Request buffer from buffer pool.
     *
     * @return requested buffer.
     */
    BufferBuilder requestBufferFromPool() throws InterruptedException;

    /**
     * This method is called when buffer should mark as released in {@link
     * RegionBufferIndexTracker}.
     *
     * @param subpartitionId the subpartition that target buffer belong to.
     * @param bufferIndex index of buffer to mark as released.
     */
    void markBufferReleasedFromFile(int subpartitionId, int bufferIndex);

    /**
     * This method is called when buffer is consumed.
     *
     * @param consumedBuffer target buffer to mark as consumed.
     */
    void onBufferConsumed(BufferIndexAndChannel consumedBuffer);

    /** This method is called when buffer is finished. */
    void onBufferFinished();

    /**
     * This method is called when subpartition data become available.
     *
     * @param subpartitionId the subpartition's identifier that this consumer belongs to.
     * @param consumerIds the consumer's identifier which need notify data available.
     */
    void onDataAvailable(int subpartitionId, Collection<ConsumerId> consumerIds);

    /**
     * This method is called when consumer is decided to released.
     *
     * @param subpartitionId the subpartition's identifier that this consumer belongs to.
     * @param consumerId the consumer's identifier which decided to be released.
     */
    void onConsumerReleased(int subpartitionId, ConsumerId consumerId);

    /**
     * This method is called when consumer is get next buffer.
     *
     * @param subpartitionId the subpartition's identifier that this consumer belongs to.
     * @param bufferIndex the index the consumer needs.
     */
    boolean isLastBufferInSegment(int subpartitionId, int bufferIndex);
}
