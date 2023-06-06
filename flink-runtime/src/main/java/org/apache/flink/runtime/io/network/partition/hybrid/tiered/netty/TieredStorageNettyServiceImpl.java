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
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.NettyPayload;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.BiConsumer;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link TieredStorageNettyServiceImpl} is used to create netty services in producer and consumer
 * side.
 */
public class TieredStorageNettyServiceImpl implements TieredStorageNettyService {

    private final Map<TieredStoragePartitionId, List<NettyProducerService>>
            registeredProducerServices = new ConcurrentHashMap<>();

    private final Map<NettyConnectionId, BufferAvailabilityListener>
            registeredAvailabilityListeners = new ConcurrentHashMap<>();

    private final Map<TieredStoragePartitionId, Map<TieredStorageSubpartitionId, Integer>>
            registeredChannelIndexes = new ConcurrentHashMap<>();

    private final Map<TieredStoragePartitionId, Map<TieredStorageSubpartitionId, InputChannel[]>>
            registeredInputChannels = new ConcurrentHashMap<>();

    private final Map<
                    TieredStoragePartitionId,
                    Map<TieredStorageSubpartitionId, BiConsumer<Integer, Boolean>>>
            registeredQueueChannelCallbacks = new ConcurrentHashMap<>();

    private final Map<TieredStoragePartitionId, Map<TieredStorageSubpartitionId, int[]>>
            registeredPriorityArrays = new ConcurrentHashMap<>();

    @Override
    public void registerProducer(
            TieredStoragePartitionId partitionId, NettyProducerService producerService) {
        List<NettyProducerService> producerServices =
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
        InputChannel[] inputChannels =
                registeredInputChannels.get(partitionId).remove(subpartitionId);
        if (registeredInputChannels.get(partitionId).isEmpty()) {
            registeredInputChannels.remove(partitionId);
        }
        BiConsumer<Integer, Boolean> queueChannelCallback =
                registeredQueueChannelCallbacks.get(partitionId).remove(subpartitionId);
        if (registeredQueueChannelCallbacks.get(partitionId).isEmpty()) {
            registeredQueueChannelCallbacks.remove(partitionId);
        }
        int[] priorityArray = registeredPriorityArrays.get(partitionId).remove(subpartitionId);
        if (registeredPriorityArrays.get(partitionId).isEmpty()) {
            registeredPriorityArrays.remove(partitionId);
        }
        return new NettyConnectionReaderImpl(
                channelIndex, inputChannels, queueChannelCallback, priorityArray);
    }

    /**
     * Create a {@link ResultSubpartitionView} for the netty server.
     *
     * @param partitionId partitionId indicates the partitionId of result partition and
     *     subpartition.
     * @param availabilityListener listener is used to listen the available status of data.
     * @return the {@link TieredStoreResultSubpartitionView}.
     */
    public ResultSubpartitionView createResultSubpartitionView(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            BufferAvailabilityListener availabilityListener) {
        List<NettyProducerService> producerServices =
                registeredProducerServices.get(partitionId);
        if (producerServices == null) {
            return new TieredStoreResultSubpartitionView(
                    availabilityListener,
                    new ArrayList<>(),
                    new ArrayList<>(),
                    new ArrayList<>());
        }
        List<Queue<NettyPayload>> queues = new ArrayList<>();
        List<NettyConnectionId> nettyConnectionIds = new ArrayList<>();
        for (NettyProducerService producerService : producerServices) {
            LinkedBlockingQueue<NettyPayload> queue = new LinkedBlockingQueue<>();
            NettyConnectionWriterImpl writer = new NettyConnectionWriterImpl(queue);
            producerService.registerNettyConnectionWriter(subpartitionId, writer);
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
     * @param id id indicates the id of result partition and subpartition.
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
     * @param inputChannels input channels is the channels in {@link SingleInputGate}.
     * @param queueChannelCallback the call back to queue channel in {@link SingleInputGate}.
     * @param lastPrioritySequenceNumber the array to record the priority sequence number.
     */
    public void setUpInputChannels(
            TieredStoragePartitionId[] partitionIds,
            TieredStorageSubpartitionId[] subpartitionIds,
            InputChannel[] inputChannels,
            BiConsumer<Integer, Boolean> queueChannelCallback,
            int[] lastPrioritySequenceNumber) {
        checkState(partitionIds.length == subpartitionIds.length);
        for (int index = 0; index < partitionIds.length; ++index) {
            TieredStoragePartitionId partitionId = partitionIds[index];
            TieredStorageSubpartitionId subpartitionId = subpartitionIds[index];
            Map<TieredStorageSubpartitionId, Integer> channelIndexes =
                    registeredChannelIndexes.getOrDefault(partitionId, new ConcurrentHashMap<>());
            channelIndexes.put(subpartitionId, index);
            registeredChannelIndexes.put(partitionId, channelIndexes);
            Map<TieredStorageSubpartitionId, InputChannel[]> allInputChannels =
                    registeredInputChannels.getOrDefault(partitionId, new ConcurrentHashMap<>());
            allInputChannels.put(subpartitionId, inputChannels);
            registeredInputChannels.put(partitionId, allInputChannels);
            Map<TieredStorageSubpartitionId, BiConsumer<Integer, Boolean>> callBacks =
                    registeredQueueChannelCallbacks.getOrDefault(
                            partitionId, new ConcurrentHashMap<>());
            callBacks.put(subpartitionId, queueChannelCallback);
            registeredQueueChannelCallbacks.put(partitionId, callBacks);
            Map<TieredStorageSubpartitionId, int[]> priorityArrays =
                    registeredPriorityArrays.getOrDefault(partitionId, new ConcurrentHashMap<>());
            priorityArrays.put(subpartitionId, lastPrioritySequenceNumber);
            registeredPriorityArrays.put(partitionId, priorityArrays);
        }
    }
}
