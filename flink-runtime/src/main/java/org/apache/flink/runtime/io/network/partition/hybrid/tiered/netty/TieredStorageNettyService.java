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

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierConsumerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierProducerAgent;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

/** {@link TieredStorageNettyService} is used to create writers and readers to netty. */
public interface TieredStorageNettyService {

    /**
     * {@link TierProducerAgent} will register to {@link TieredStorageNettyService} and create a
     * {@link NettyConnectionWriter}.
     *
     * @param id id is used as the unique id of writer.
     * @return the writer.
     */
    void registerProducer(
            TieredStoragePartitionId partitionId,
            BiConsumer<TieredStorageSubpartitionId, NettyConnectionWriter> writerRegisterCallback,
            Consumer<NettyConnectionId> connectionDisconnectedListener);

    /**
     * {@link TierConsumerAgent} will register to {@link TieredStorageNettyService} and create a
     * {@link NettyConnectionReader}.
     *
     * @param id id is used as the unique id of reader.
     * @return the reader.
     */
    NettyConnectionReader registerConsumer(
            TieredStoragePartitionId partitionId, TieredStorageSubpartitionId subpartitionId);
}
