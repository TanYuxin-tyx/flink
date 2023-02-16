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

package org.apache.flink.runtime.io.network.partition.store.tier.local.memory;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.partition.store.common.TierReaderId;

import java.util.Collection;

/**
 * This interface is used by {@link SubpartitionConsumerMemoryDataManager} to operate {@link
 * MemoryDataWriter}. Spilling decision may be made and handled inside these operations.
 */
public interface MemoryDataWriterOperation {
    /**
     * Request buffer from buffer pool.
     *
     * @return requested buffer.
     */
    MemorySegment requestBufferFromPool(int subpartitionId) throws InterruptedException;

    /**
     * This method is called when subpartition data become available.
     *
     * @param subpartitionId the subpartition's identifier that this consumer belongs to.
     * @param tierReaderIds the consumer's identifier which need notify data available.
     */
    void onDataAvailable(int subpartitionId, Collection<TierReaderId> tierReaderIds);

    /**
     * This method is called when consumer is decided to released.
     *
     * @param subpartitionId the subpartition's identifier that this consumer belongs to.
     * @param tierReaderId the consumer's identifier which decided to be released.
     */
    void onConsumerReleased(int subpartitionId, TierReaderId tierReaderId);

    boolean isConsumerRegistered(int subpartitionId);
}
