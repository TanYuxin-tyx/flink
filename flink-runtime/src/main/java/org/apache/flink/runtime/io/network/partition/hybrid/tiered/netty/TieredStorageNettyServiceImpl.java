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

import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.shuffle.TieredResultPartition;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageConsumerSpec;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageResourceRegistry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkState;

/** The default implementation of {@link TieredStorageNettyService}. */
public class TieredStorageNettyServiceImpl implements TieredStorageNettyService {

    // ------------------------------------
    //          For producer side
    // ------------------------------------

    private final Map<TieredStoragePartitionId, List<NettyServiceProducer>>
            registeredServiceProducers = new ConcurrentHashMap<>();

    private final Map<NettyConnectionId, BufferAvailabilityListener>
            registeredAvailabilityListeners = new ConcurrentHashMap<>();

    // ------------------------------------
    //          For consumer side
    // ------------------------------------

    private final Map<
                    TieredStoragePartitionId,
                    Map<TieredStorageSubpartitionId, CompletableFuture<NettyConnectionReader>>>
            registeredNettyConnectionReaders = new HashMap<>();

    private final TieredStorageResourceRegistry resourceRegistry;

    public TieredStorageNettyServiceImpl(TieredStorageResourceRegistry resourceRegistry) {
        this.resourceRegistry = resourceRegistry;
    }

    @Override
    public void registerProducer(
            TieredStoragePartitionId partitionId, NettyServiceProducer serviceProducer) {
        registeredServiceProducers
                .computeIfAbsent(
                        partitionId,
                        ignore -> {
                            final TieredStoragePartitionId id = partitionId;
                            resourceRegistry.registerResource(
                                    id, () -> registeredServiceProducers.remove(id));
                            return new ArrayList<>();
                        })
                .add(serviceProducer);
    }

    @Override
    public CompletableFuture<NettyConnectionReader> registerConsumer(
            TieredStoragePartitionId partitionId, TieredStorageSubpartitionId subpartitionId) {
        return registeredNettyConnectionReaders
                .computeIfAbsent(partitionId, ignored -> new HashMap<>())
                .computeIfAbsent(subpartitionId, ignored -> new CompletableFuture<>());
    }

    /**
     * Create a {@link ResultSubpartitionView} for the netty server.
     *
     * @param partitionId partition id indicates the unique id of {@link TieredResultPartition}.
     * @param subpartitionId subpartition id indicates the unique id of subpartition.
     * @param availabilityListener listener is used to listen the available status of data.
     * @return the {@link TieredStorageResultSubpartitionView}.
     */
    public ResultSubpartitionView createResultSubpartitionView(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            BufferAvailabilityListener availabilityListener) {
        List<NettyServiceProducer> serviceProducers = registeredServiceProducers.get(partitionId);
        if (serviceProducers == null) {
            return new TieredStorageResultSubpartitionView(
                    availabilityListener, new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
        }
        List<Queue<NettyPayload>> queues = new ArrayList<>();
        List<NettyConnectionId> nettyConnectionIds = new ArrayList<>();
        for (NettyServiceProducer serviceProducer : serviceProducers) {
            LinkedBlockingQueue<NettyPayload> queue = new LinkedBlockingQueue<>();
            NettyConnectionWriterImpl writer = new NettyConnectionWriterImpl(queue);
            serviceProducer.connectionEstablished(subpartitionId, writer);
            nettyConnectionIds.add(writer.getNettyConnectionId());
            queues.add(queue);
            registeredAvailabilityListeners.put(
                    writer.getNettyConnectionId(), availabilityListener);
        }
        return new TieredStorageResultSubpartitionView(
                availabilityListener,
                queues,
                nettyConnectionIds,
                registeredServiceProducers.get(partitionId));
    }

    /**
     * Notify the {@link ResultSubpartitionView} to send buffer.
     *
     * @param connectionId connection id indicates the id of connection.
     */
    public void notifyResultSubpartitionViewSendBuffer(NettyConnectionId connectionId) {
        BufferAvailabilityListener listener = registeredAvailabilityListeners.get(connectionId);
        if (listener != null) {
            listener.notifyDataAvailable();
        }
    }

    public void setupInputChannels(
            List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs,
            List<Supplier<InputChannel>> inputChannelProviders,
            NettyConnectionReaderAvailabilityAndPriorityHelper helper) {
        checkState(tieredStorageConsumerSpecs.size() == inputChannelProviders.size());
        for (int index = 0; index < tieredStorageConsumerSpecs.size(); ++index) {
            TieredStorageConsumerSpec spec = tieredStorageConsumerSpecs.get(index);
            TieredStoragePartitionId partitionId = spec.getPartitionId();
            TieredStorageSubpartitionId subpartitionId = spec.getSubpartitionId();
            registeredNettyConnectionReaders
                    .remove(partitionId)
                    .remove(subpartitionId)
                    .complete(
                            new NettyConnectionReaderImpl(
                                    index, inputChannelProviders.get(index), helper));
        }
    }
}
