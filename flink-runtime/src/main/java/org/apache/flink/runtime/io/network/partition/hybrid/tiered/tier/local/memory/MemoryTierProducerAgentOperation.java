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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.local.memory;

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.service.NettyServiceViewId;

import java.util.Collection;

/**
 * This interface is used by {@link MemoryTierConsumer} to operate {@link
 * MemoryTierProducerAgentWriter}. Spilling decision may be made and handled inside these
 * operations.
 */
public interface MemoryTierProducerAgentOperation {
    /**
     * This method is called when subpartition data become available.
     *
     * @param subpartitionId the subpartition's identifier that this consumer belongs to.
     * @param nettyServiceViewIds the consumer's identifier which need notify data available.
     */
    void onDataAvailable(int subpartitionId, Collection<NettyServiceViewId> nettyServiceViewIds);

    /**
     * This method is called when consumer is decided to released.
     *
     * @param subpartitionId the subpartition's identifier that this consumer belongs to.
     * @param nettyServiceViewId the consumer's identifier which decided to be released.
     */
    void onConsumerReleased(int subpartitionId, NettyServiceViewId nettyServiceViewId);

    boolean isConsumerRegistered(int subpartitionId);
}