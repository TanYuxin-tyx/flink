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

package org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk;

import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.BufferIndexAndChannel;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierReaderViewId;

import java.util.Collection;
import java.util.Deque;

/**
 * This interface is used by operate. Spilling decision may be made and handled inside these operations.
 */
public interface DiskCacheManagerOperation {
    /**
     * Get the number of downstream consumers.
     *
     * @return Number of subpartitions.
     */
    int getNumSubpartitions();

    /** Get all buffers from the subpartition. */
    Deque<BufferIndexAndChannel> getBuffersInOrder(int subpartitionId);

    /**
     * Get the current size of buffer pool. *
     *
     * <p>/** Request buffer from buffer pool.
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
     * This method is called when subpartition data become available.
     *
     * @param subpartitionId the subpartition's identifier that this consumer belongs to.
     * @param tierReaderViewIds the consumer's identifier which need notify data available.
     */
    void onDataAvailable(int subpartitionId, Collection<TierReaderViewId> tierReaderViewIds);

    /**
     * This method is called when consumer is decided to released.
     *
     * @param subpartitionId the subpartition's identifier that this consumer belongs to.
     * @param tierReaderViewId the consumer's identifier which decided to be released.
     */
    void onConsumerReleased(int subpartitionId, TierReaderViewId tierReaderViewId);

    /**
     * This method is called when consumer is get next buffer.
     *
     * @param subpartitionId the subpartition's identifier that this consumer belongs to.
     * @param bufferIndex the index the consumer needs.
     */
    boolean isLastBufferInSegment(int subpartitionId, int bufferIndex);

    enum SpillStatus {
        /** The buffer is spilling or spilled already. */
        SPILL,
        /** The buffer not start spilling. */
        NOT_SPILL,
    }

    /** This enum represents buffer status of consume in hybrid shuffle mode. */
    enum ConsumeStatus {
        /** The buffer has already consumed by downstream. */
        CONSUMED,
        /** The buffer is not consumed by downstream. */
        NOT_CONSUMED,
        /** The buffer is either consumed or not consumed. */
        ALL
    }

    /** This class represents a pair of {@link ConsumeStatus} and consumer id. */
    class ConsumeStatusWithId {
        public static final ConsumeStatusWithId ALL_ANY =
                new ConsumeStatusWithId(ConsumeStatus.ALL, TierReaderViewId.ANY);

        ConsumeStatus status;

        TierReaderViewId tierReaderViewId;

        private ConsumeStatusWithId(ConsumeStatus status, TierReaderViewId tierReaderViewId) {
            this.status = status;
            this.tierReaderViewId = tierReaderViewId;
        }

        public static ConsumeStatusWithId fromStatusAndConsumerId(
                ConsumeStatus consumeStatus, TierReaderViewId tierReaderViewId) {
            return new ConsumeStatusWithId(consumeStatus, tierReaderViewId);
        }

        public TierReaderViewId getConsumerId() {
            return tierReaderViewId;
        }

        public ConsumeStatus getStatus() {
            return status;
        }
    }
}
