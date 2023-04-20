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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.local.memory;

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.service.NettyBasedTierConsumerImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.service.NettyBasedTierConsumerViewId;

import java.util.concurrent.locks.Lock;

/** The {@link MemoryTierConsumer} is used to consume data from Memory Tier. */
public class MemoryTierConsumer extends NettyBasedTierConsumerImpl {

    private final NettyBasedTierConsumerViewId nettyBasedTierConsumerViewId;

    private final int subpartitionId;

    private final MemoryTierProducerAgentOperation memoryTierProducerAgentOperation;

    public MemoryTierConsumer(
            Lock consumerLock,
            int subpartitionId,
            NettyBasedTierConsumerViewId nettyBasedTierConsumerViewId,
            MemoryTierProducerAgentOperation memoryTierProducerAgentOperation) {
        super(consumerLock);
        this.subpartitionId = subpartitionId;
        this.nettyBasedTierConsumerViewId = nettyBasedTierConsumerViewId;
        this.memoryTierProducerAgentOperation = memoryTierProducerAgentOperation;
    }

    @Override
    public void release() {
        memoryTierProducerAgentOperation.onConsumerReleased(subpartitionId, nettyBasedTierConsumerViewId);
    }
}
