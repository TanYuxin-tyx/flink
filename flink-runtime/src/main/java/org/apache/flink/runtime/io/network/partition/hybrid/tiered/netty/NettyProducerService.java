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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty;

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierProducerAgent;

/**
 * {@link NettyProducerService} is used as the callback to register {@link NettyConnectionWriter}
 * and disconnect netty connection in {@link TierProducerAgent}.
 */
public interface NettyProducerService {

    /**
     * Register a {@link NettyConnectionWriter} for a subpartition.
     *
     * @param subpartitionId subpartition id indicates the id of subpartition.
     * @param nettyConnectionWriter writer is used to write buffers to netty connection.
     */
    void registerNettyConnectionWriter(
            TieredStorageSubpartitionId subpartitionId,
            NettyConnectionWriter nettyConnectionWriter);

    /**
     * Disconnect the netty connection related to the {@link NettyConnectionId}.
     *
     * @param connectionId connection id is the id of connection.
     */
    void disconnectNettyConnection(NettyConnectionId connectionId);
}
