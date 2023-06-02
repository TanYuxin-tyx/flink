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
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyServiceReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyServiceReaderId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyServiceWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyServiceWriterId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.BufferContext;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.SegmentSearcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.BiConsumer;

import static org.apache.flink.shaded.guava30.com.google.common.base.Preconditions.checkState;

/**
 * {@link TieredStorageNettyServiceImpl} is used to create netty services in producer and consumer
 * side.
 */
public class TieredStorageNettyServiceImpl implements TieredStorageNettyService {

    private final Map<NettyServiceWriterId, List<Queue<BufferContext>>> registeredBufferQueues =
            new ConcurrentHashMap<>();

    private final Map<NettyServiceWriterId, List<Runnable>> registeredReleaseNotifiers =
            new ConcurrentHashMap<>();

    private final Map<NettyServiceWriterId, BufferAvailabilityListener>
            registeredAvailabilityListeners = new ConcurrentHashMap<>();

    @Override
    public NettyServiceWriter registerProducer(
            NettyServiceWriterId writerId, Runnable serviceReleaseNotifier) {
        Queue<BufferContext> bufferQueue = new LinkedBlockingQueue<>();
        List<Queue<BufferContext>> bufferQueues =
                registeredBufferQueues.getOrDefault(writerId, new ArrayList<>());
        List<Runnable> notifiers =
                registeredReleaseNotifiers.getOrDefault(writerId, new ArrayList<>());
        bufferQueues.add(bufferQueue);
        notifiers.add(serviceReleaseNotifier);
        registeredBufferQueues.put(writerId, bufferQueues);
        registeredReleaseNotifiers.put(writerId, notifiers);
        return new NettyServiceWriterImpl(bufferQueue);
    }

    @Override
    public NettyServiceReader registerConsumer(
            InputChannel[] inputChannels,
            BiConsumer<Integer, Boolean> subpartitionAvailableNotifier,
            int[] lastPrioritySequenceNumber) {
        return new NettyServiceReaderImpl(
                inputChannels, subpartitionAvailableNotifier, lastPrioritySequenceNumber);
    }

    @Override
    public NettyServiceReader registerConsumer(NettyServiceReaderId readerId) {
        return null;
    }

    /**
     * Create a {@link ResultSubpartitionView} for the netty server.
     *
     * @param subpartitionId subpartition id indicates the id of subpartition.
     * @param writerId writer id indicates the id of {@link NettyServiceWriter}.
     * @param availabilityListener listener is used to listen the available status of data.
     * @param segmentSearchers searcher is used to search the existence of segment in each tier.
     * @return the {@link ResultSubpartitionView}.
     */
    public ResultSubpartitionView createResultSubpartitionView(
            int subpartitionId,
            NettyServiceWriterId writerId,
            BufferAvailabilityListener availabilityListener,
            List<SegmentSearcher> segmentSearchers) {
        List<Queue<BufferContext>> bufferQueues =
                registeredBufferQueues.getOrDefault(writerId, new ArrayList<>());
        List<Runnable> releaseNotifiers =
                registeredReleaseNotifiers.getOrDefault(writerId, new ArrayList<>());
        checkState(bufferQueues.size() == releaseNotifiers.size());
        registeredAvailabilityListeners.put(writerId, availabilityListener);
        return new TieredStoreResultSubpartitionView(
                subpartitionId,
                availabilityListener,
                segmentSearchers,
                bufferQueues,
                releaseNotifiers);
    }

    /**
     * Notify the {@link ResultSubpartitionView} to send buffer.
     *
     * @param writerId writer id indicates the id of {@link NettyServiceWriter}.
     */
    public void notifyResultSubpartitionViewSendBuffer(NettyServiceWriterId writerId) {
        BufferAvailabilityListener listener = registeredAvailabilityListeners.get(writerId);
        if (listener != null) {
            listener.notifyDataAvailable();
        }
    }
}
