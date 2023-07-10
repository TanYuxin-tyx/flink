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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.memory;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.AvailabilityAndPriorityNotifier;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierConsumerAgent;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The data client is used to fetch data from memory tier. */
public class MemoryTierConsumerAgent implements TierConsumerAgent {
    private final Map<
            TieredStoragePartitionId,
            Map<TieredStorageSubpartitionId, Tuple2<CompletableFuture<NettyConnectionReader>, Integer>>>
            nettyConnectionReaders;

    private AvailabilityAndPriorityNotifier notifier;

    public MemoryTierConsumerAgent(
            Map<
                    TieredStoragePartitionId,
                    Map<TieredStorageSubpartitionId, Tuple2<CompletableFuture<NettyConnectionReader>, Integer>>>
                    nettyConnectionReaders) {
        this.nettyConnectionReaders = nettyConnectionReaders;
    }

    @Override
    public void start() {
        // nothing to do.
    }

    @Override
    public void registerAvailabilityAndPriorityNotifier(AvailabilityAndPriorityNotifier notifier) {
        this.notifier = notifier;
    }

    @Override
    public Optional<Buffer> getNextBuffer(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId) {
        Optional<Buffer> buffer = Optional.empty();
        Tuple2<CompletableFuture<NettyConnectionReader>, Integer> readerAndBufferIndex =
                nettyConnectionReaders.get(partitionId).get(subpartitionId);
        try {
            buffer = readerAndBufferIndex.f0.get().readBuffer(segmentId);
        } catch (InterruptedException | ExecutionException e) {
            ExceptionUtils.rethrow(e, "Failed to read buffer from memory tier.");
        }
        buffer.ifPresent(
                value -> {
                    boolean isPriority = value.getDataType().hasPriority();
                    checkNotNull(notifier)
                            .notifyAvailableAndPriority(
                                    partitionId,
                                    subpartitionId,
                                    isPriority,
                                    isPriority ? readerAndBufferIndex.f1 : null);
                    readerAndBufferIndex.f1 += 1;
                });
        return buffer;
    }
    @Override
    public void close() throws IOException {
        // nothing to do.
    }
}
