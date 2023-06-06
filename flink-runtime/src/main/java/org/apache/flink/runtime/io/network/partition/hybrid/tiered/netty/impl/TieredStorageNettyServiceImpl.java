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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.impl;

import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStoragePartitionIdAndSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.BufferContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * {@link TieredStorageNettyServiceImpl} is used to create netty services in producer and consumer
 * side.
 */
public class TieredStorageNettyServiceImpl implements TieredStorageNettyService {

    private final Map<TieredStoragePartitionIdAndSubpartitionId, List<Queue<BufferContext>>>
            registeredBufferQueues = new ConcurrentHashMap<>();

    private final Map<TieredStoragePartitionIdAndSubpartitionId, List<Runnable>>
            registeredReleaseNotifiers = new ConcurrentHashMap<>();

    private final Map<NettyConnectionId, BufferAvailabilityListener>
            registeredAvailabilityListeners = new ConcurrentHashMap<>();

    private final Map<TieredStoragePartitionIdAndSubpartitionId, Integer> registeredChannelIndexes =
            new ConcurrentHashMap<>();

    private final Map<TieredStoragePartitionIdAndSubpartitionId, InputChannel[]>
            registeredInputChannels = new ConcurrentHashMap<>();

    private final Map<TieredStoragePartitionIdAndSubpartitionId, BiConsumer<Integer, Boolean>>
            registeredQueueChannelCallbacks = new ConcurrentHashMap<>();

    private final Map<TieredStoragePartitionIdAndSubpartitionId, int[]> registeredPriorityArrays =
            new ConcurrentHashMap<>();
    private final Map<
                    TieredStoragePartitionId,
                    List<BiConsumer<TieredStorageSubpartitionId, NettyConnectionWriter>>>
            registeredWriterCallBacks = new ConcurrentHashMap<>();
    private final Map<TieredStoragePartitionId, List<Consumer<NettyConnectionId>>>
            registeredConnectionDisconnectedListener = new ConcurrentHashMap<>();

    @Override
    public void registerProducer(
            TieredStoragePartitionId partitionId,
            BiConsumer<TieredStorageSubpartitionId, NettyConnectionWriter> writerRegisterCallback,
            Consumer<NettyConnectionId> connectionDisconnectedListener) {
        List<BiConsumer<TieredStorageSubpartitionId, NettyConnectionWriter>> callBacks =
                registeredWriterCallBacks.getOrDefault(partitionId, new ArrayList<>());
        callBacks.add(writerRegisterCallback);
        registeredWriterCallBacks.put(partitionId, callBacks);
        List<Consumer<NettyConnectionId>> listeners =
                registeredConnectionDisconnectedListener.getOrDefault(
                        partitionId, new ArrayList<>());
        listeners.add(connectionDisconnectedListener);
        registeredConnectionDisconnectedListener.put(partitionId, listeners);
    }

    @Override
    public NettyConnectionReader registerConsumer(TieredStoragePartitionIdAndSubpartitionId id) {
        return new NettyConnectionReaderImpl(
                registeredChannelIndexes.remove(id),
                registeredInputChannels.remove(id),
                registeredQueueChannelCallbacks.remove(id),
                registeredPriorityArrays.remove(id));
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
        List<BiConsumer<TieredStorageSubpartitionId, NettyConnectionWriter>> callBacks =
                registeredWriterCallBacks.get(partitionId);
        if (callBacks == null) {
            return new TieredStoreResultSubpartitionView(
                    subpartitionId,
                    availabilityListener,
                    new ArrayList<>(),
                    new ArrayList<>(),
                    new ArrayList<>());
        }
        List<Queue<BufferContext>> queues = new ArrayList<>();
        List<NettyConnectionId> nettyConnectionIds = new ArrayList<>();
        for (BiConsumer<TieredStorageSubpartitionId, NettyConnectionWriter> callBack : callBacks) {
            LinkedBlockingQueue<BufferContext> queue = new LinkedBlockingQueue<>();
            NettyConnectionWriterImpl writer = new NettyConnectionWriterImpl(queue);
            callBack.accept(subpartitionId, writer);
            nettyConnectionIds.add(writer.getNettyConnectionId());
            queues.add(queue);
            registeredAvailabilityListeners.put(
                    writer.getNettyConnectionId(), availabilityListener);
        }
        return new TieredStoreResultSubpartitionView(
                subpartitionId,
                availabilityListener,
                queues,
                nettyConnectionIds,
                registeredConnectionDisconnectedListener.get(partitionId));
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
            TieredStoragePartitionIdAndSubpartitionId[] ids,
            InputChannel[] inputChannels,
            BiConsumer<Integer, Boolean> queueChannelCallback,
            int[] lastPrioritySequenceNumber) {
        for (int index = 0; index < ids.length; ++index) {
            TieredStoragePartitionIdAndSubpartitionId id = ids[index];
            registeredChannelIndexes.put(id, index);
            registeredInputChannels.put(id, inputChannels);
            registeredQueueChannelCallbacks.put(id, queueChannelCallback);
            registeredPriorityArrays.put(id, lastPrioritySequenceNumber);
        }
    }
}
