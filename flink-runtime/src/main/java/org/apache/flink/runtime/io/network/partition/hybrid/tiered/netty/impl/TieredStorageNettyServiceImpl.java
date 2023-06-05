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
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStoragePartitionIdAndSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.BufferContext;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.BiConsumer;

import static org.apache.flink.shaded.guava30.com.google.common.base.Preconditions.checkState;

/**
 * {@link TieredStorageNettyServiceImpl} is used to create netty services in producer and consumer
 * side.
 */
public class TieredStorageNettyServiceImpl implements TieredStorageNettyService {

    private final Map<TieredStoragePartitionIdAndSubpartitionId, List<Queue<BufferContext>>>
            registeredBufferQueues = new ConcurrentHashMap<>();

    private final Map<TieredStoragePartitionIdAndSubpartitionId, List<Runnable>>
            registeredReleaseNotifiers = new ConcurrentHashMap<>();

    private final Map<TieredStoragePartitionIdAndSubpartitionId, BufferAvailabilityListener>
            registeredAvailabilityListeners = new ConcurrentHashMap<>();

    private final Map<TieredStoragePartitionIdAndSubpartitionId, Integer> registeredChannelIndexes =
            new ConcurrentHashMap<>();

    private final Map<TieredStoragePartitionIdAndSubpartitionId, InputChannel[]>
            registeredInputChannels = new ConcurrentHashMap<>();

    private final Map<TieredStoragePartitionIdAndSubpartitionId, BiConsumer<Integer, Boolean>>
            registeredQueueChannelCallbacks = new ConcurrentHashMap<>();

    private final Map<TieredStoragePartitionIdAndSubpartitionId, int[]> registeredPriorityArrays =
            new ConcurrentHashMap<>();

    @Override
    public NettyConnectionWriter registerProducer(
            TieredStoragePartitionIdAndSubpartitionId id, Runnable serviceReleaseNotifier) {
        Queue<BufferContext> bufferQueue = new LinkedBlockingQueue<>();
        List<Queue<BufferContext>> bufferQueues =
                registeredBufferQueues.getOrDefault(id, new CopyOnWriteArrayList<>());
        List<Runnable> notifiers =
                registeredReleaseNotifiers.getOrDefault(id, new CopyOnWriteArrayList<>());
        bufferQueues.add(bufferQueue);
        notifiers.add(serviceReleaseNotifier);
        registeredBufferQueues.put(id, bufferQueues);
        registeredReleaseNotifiers.put(id, notifiers);
        return new NettyConnectionWriterImpl(bufferQueue);
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
     * @param subpartitionId subpartition id indicates the id of subpartition.
     * @param id writer id indicates the id of {@link NettyConnectionWriter}.
     * @param availabilityListener listener is used to listen the available status of data.
     * @param segmentSearchers searcher is used to search the existence of segment in each tier.
     * @return the {@link ResultSubpartitionView}.
     */
    public ResultSubpartitionView createResultSubpartitionView(
            int subpartitionId,
            TieredStoragePartitionIdAndSubpartitionId id,
            BufferAvailabilityListener availabilityListener) {
        List<Queue<BufferContext>> bufferQueues =
                registeredBufferQueues.getOrDefault(id, new CopyOnWriteArrayList<>());
        List<Runnable> releaseNotifiers =
                registeredReleaseNotifiers.getOrDefault(id, new CopyOnWriteArrayList<>());
        checkState(bufferQueues.size() == releaseNotifiers.size());
        registeredBufferQueues.remove(id);
        registeredReleaseNotifiers.remove(id);
        registeredAvailabilityListeners.put(id, availabilityListener);
        return new TieredStoreResultSubpartitionView(
                subpartitionId, availabilityListener, bufferQueues, releaseNotifiers);
    }

    /**
     * Notify the {@link ResultSubpartitionView} to send buffer.
     *
     * @param writerId writer id indicates the id of {@link NettyConnectionWriter}.
     */
    public void notifyResultSubpartitionViewSendBuffer(
            TieredStoragePartitionIdAndSubpartitionId writerId) {
        BufferAvailabilityListener listener = registeredAvailabilityListeners.get(writerId);
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
