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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.disk;

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

/** The data client is used to fetch data from disk tier. */
public class DiskTierConsumerAgent implements TierConsumerAgent {
    private final Map<
                    TieredStoragePartitionId,
                    Map<TieredStorageSubpartitionId, CompletableFuture<NettyConnectionReader>>>
            readers;

    private AvailabilityAndPriorityNotifier notifier;

    public DiskTierConsumerAgent(
            Map<
                            TieredStoragePartitionId,
                            Map<
                                    TieredStorageSubpartitionId,
                                    CompletableFuture<NettyConnectionReader>>>
                    readers) {
        this.readers = readers;
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
        try {
            buffer = readers.get(partitionId).get(subpartitionId).get().readBuffer(segmentId);
        } catch (InterruptedException | ExecutionException e) {
            ExceptionUtils.rethrow(e, "Failed to read buffer from disk tier.");
        }
        buffer.ifPresent(
                value ->
                        notifier.notifyAvailableAndPriority(
                                partitionId, subpartitionId, value.getDataType().hasPriority()));
        return buffer;
    }

    @Override
    public void close() throws IOException {
        // nothing to do.
    }
}
