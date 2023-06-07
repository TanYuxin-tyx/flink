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
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.shuffle.TieredResultPartition;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.NettyPayload;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkState;

/** The default implementation of {@link TieredStorageNettyService}. */
public class TieredStorageNettyServiceImpl implements TieredStorageNettyService {

    // ------------------------------------
    //          For producer side
    // ------------------------------------

    private final Map<TieredStoragePartitionId, List<NettyServiceProducer>>
            registeredProducerServices = new ConcurrentHashMap<>();

    private final Map<NettyConnectionId, BufferAvailabilityListener>
            registeredAvailabilityListeners = new ConcurrentHashMap<>();

    private final Map<TieredStoragePartitionId, Map<TieredStorageSubpartitionId, Integer>>
            registeredChannelIndexes = new ConcurrentHashMap<>();

    // ------------------------------------
    //          For consumer side
    // ------------------------------------

    private final Map<
                    TieredStoragePartitionId,
                    Map<TieredStorageSubpartitionId, Function<Integer, InputChannel>>>
            registeredInputChannelProviders = new ConcurrentHashMap<>();

    private final Map<
                    TieredStoragePartitionId,
                    Map<
                            TieredStorageSubpartitionId,
                            NettyConnectionReaderAvailabilityAndPriorityHelper>>
            registeredNettyConnectionReaderAvailabilityAndPriorityHelpers =
                    new ConcurrentHashMap<>();

    @Override
    public void registerProducer(
            TieredStoragePartitionId partitionId, NettyServiceProducer producerService) {
        List<NettyServiceProducer> producerServices =
                registeredProducerServices.getOrDefault(partitionId, new ArrayList<>());
        producerServices.add(producerService);
        registeredProducerServices.put(partitionId, producerServices);
    }

    @Override
    public NettyConnectionReader registerConsumer(
            TieredStoragePartitionId partitionId, TieredStorageSubpartitionId subpartitionId) {
        Integer channelIndex = registeredChannelIndexes.get(partitionId).remove(subpartitionId);
        if (registeredChannelIndexes.get(partitionId).isEmpty()) {
            registeredChannelIndexes.remove(partitionId);
        }

        Function<Integer, InputChannel> inputChannelProvider =
                registeredInputChannelProviders.get(partitionId).remove(subpartitionId);
        if (registeredInputChannelProviders.get(partitionId).isEmpty()) {
            registeredInputChannelProviders.remove(partitionId);
        }

        NettyConnectionReaderAvailabilityAndPriorityHelper helper =
                registeredNettyConnectionReaderAvailabilityAndPriorityHelpers
                        .get(partitionId)
                        .remove(subpartitionId);
        if (registeredNettyConnectionReaderAvailabilityAndPriorityHelpers
                .get(partitionId)
                .isEmpty()) {
            registeredNettyConnectionReaderAvailabilityAndPriorityHelpers.remove(partitionId);
        }
        return new NettyConnectionReaderImpl(channelIndex, inputChannelProvider, helper);
    }

    /**
     * Create a {@link ResultSubpartitionView} for the netty server.
     *
     * @param partitionId partition id indicates the unique id of {@link TieredResultPartition}.
     * @param subpartitionId subpartition id indicates the unique id of subpartition.
     * @param availabilityListener listener is used to listen the available status of data.
     * @return the {@link TieredStoreResultSubpartitionView}.
     */
    public ResultSubpartitionView createResultSubpartitionView(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            BufferAvailabilityListener availabilityListener) {
        List<NettyServiceProducer> producerServices = registeredProducerServices.get(partitionId);
        if (producerServices == null) {
            return new TieredStoreResultSubpartitionView(
                    availabilityListener, new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
        }
        List<Queue<NettyPayload>> queues = new ArrayList<>();
        List<NettyConnectionId> nettyConnectionIds = new ArrayList<>();
        for (NettyServiceProducer producerService : producerServices) {
            LinkedBlockingQueue<NettyPayload> queue = new LinkedBlockingQueue<>();
            NettyConnectionWriterImpl writer = new NettyConnectionWriterImpl(queue);
            producerService.connectionEstablished(subpartitionId, writer);
            nettyConnectionIds.add(writer.getNettyConnectionId());
            queues.add(queue);
            registeredAvailabilityListeners.put(
                    writer.getNettyConnectionId(), availabilityListener);
        }
        return new TieredStoreResultSubpartitionView(
                availabilityListener,
                queues,
                nettyConnectionIds,
                registeredProducerServices.get(partitionId));
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

    /**
     * Set up input channels in {@link SingleInputGate}.
     *
     * @param partitionIds partition ids indicates the ids of {@link TieredResultPartition}.
     * @param subpartitionIds subpartition ids indicates the ids of subpartition.
     */
    public void setUpInputChannels(
            TieredStoragePartitionId[] partitionIds,
            TieredStorageSubpartitionId[] subpartitionIds,
            Function<Integer, InputChannel> inputChannelProvider,
            NettyConnectionReaderAvailabilityAndPriorityHelper helper) {
        checkState(partitionIds.length == subpartitionIds.length);
        for (int index = 0; index < partitionIds.length; ++index) {
            TieredStoragePartitionId partitionId = partitionIds[index];
            TieredStorageSubpartitionId subpartitionId = subpartitionIds[index];

            Map<TieredStorageSubpartitionId, Integer> channelIndexes =
                    registeredChannelIndexes.getOrDefault(partitionId, new ConcurrentHashMap<>());
            channelIndexes.put(subpartitionId, index);
            registeredChannelIndexes.put(partitionId, channelIndexes);

            Map<TieredStorageSubpartitionId, Function<Integer, InputChannel>>
                    inputChannelProviders =
                            registeredInputChannelProviders.getOrDefault(
                                    partitionId, new ConcurrentHashMap<>());
            inputChannelProviders.put(subpartitionId, inputChannelProvider);
            registeredInputChannelProviders.put(partitionId, inputChannelProviders);

            Map<TieredStorageSubpartitionId, NettyConnectionReaderAvailabilityAndPriorityHelper>
                    helpers =
                            registeredNettyConnectionReaderAvailabilityAndPriorityHelpers
                                    .getOrDefault(partitionId, new ConcurrentHashMap<>());
            helpers.put(subpartitionId, helper);
            registeredNettyConnectionReaderAvailabilityAndPriorityHelpers.put(partitionId, helpers);
        }
    }
}
