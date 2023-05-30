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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.netty2.impl;

import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.CreditBasedBufferQueueView;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.CreditBasedBufferQueueViewImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStoreResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.netty2.NettyServiceReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.netty2.NettyServiceWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.netty2.TieredStorageNettyService2;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.BufferContext;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.SegmentSearcher;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.BiConsumer;

import static org.apache.flink.shaded.guava30.com.google.common.base.Preconditions.checkState;

/**
 * {@link TieredStorageNettyServiceImpl2} is used to create netty services in producer and consumer
 * side.
 */
public class TieredStorageNettyServiceImpl2 implements TieredStorageNettyService2 {

    private final Map<ResultPartitionID, Map<Integer, List<Queue<BufferContext>>>>
            registeredBufferQueues = new ConcurrentHashMap<>();

    private final Map<ResultPartitionID, Map<Integer, List<Runnable>>> registeredReleaseNotifiers =
            new ConcurrentHashMap<>();

    private final Map<ResultPartitionID, Map<Integer, BufferAvailabilityListener>>
            registeredAvailabilityListeners = new ConcurrentHashMap<>();

    @Override
    public NettyServiceWriter registerProducer(
            ResultPartitionID partitionId, int subpartitionId, Runnable serviceReleaseNotifier) {
        synchronized (this) {
            Queue<BufferContext> bufferQueue = new LinkedBlockingQueue<>();
            Map<Integer, List<Queue<BufferContext>>> subpartitionBufferQueues =
                    registeredBufferQueues.getOrDefault(partitionId, new HashMap<>());
            Map<Integer, List<Runnable>> subpartitionReleaseNotifiers =
                    registeredReleaseNotifiers.getOrDefault(partitionId, new HashMap<>());
            List<Queue<BufferContext>> bufferQueues =
                    subpartitionBufferQueues.getOrDefault(subpartitionId, new ArrayList<>());
            List<Runnable> notifiers =
                    subpartitionReleaseNotifiers.getOrDefault(subpartitionId, new ArrayList<>());
            bufferQueues.add(bufferQueue);
            notifiers.add(serviceReleaseNotifier);
            subpartitionBufferQueues.put(subpartitionId, bufferQueues);
            subpartitionReleaseNotifiers.put(subpartitionId, notifiers);
            registeredBufferQueues.put(partitionId, subpartitionBufferQueues);
            registeredReleaseNotifiers.put(partitionId, subpartitionReleaseNotifiers);
            return new NettyServiceWriterImpl(bufferQueue);
        }
    }

    @Override
    public NettyServiceReader registerConsumer(
            InputChannel[] inputChannels,
            BiConsumer<Integer, Boolean> subpartitionAvailableNotifier,
            int[] lastPrioritySequenceNumber) {
        return new NettyServiceReaderImpl(
                inputChannels, subpartitionAvailableNotifier, lastPrioritySequenceNumber);
    }

    public ResultSubpartitionView createResultSubpartitionView(
            ResultPartitionID partitionId,
            int subpartitionId,
            BufferAvailabilityListener availabilityListener,
            List<SegmentSearcher> segmentSearchers) {
        synchronized (this) {
            List<Queue<BufferContext>> bufferQueues =
                    registeredBufferQueues.get(partitionId).get(subpartitionId);
            List<Runnable> releaseNotifiers =
                    registeredReleaseNotifiers.get(partitionId).get(subpartitionId);
            checkState(bufferQueues.size() != 0 && bufferQueues.size() == releaseNotifiers.size());
            Map<Integer, BufferAvailabilityListener> listeners =
                    registeredAvailabilityListeners.getOrDefault(partitionId, new HashMap<>());
            listeners.put(subpartitionId, availabilityListener);
            registeredAvailabilityListeners.put(partitionId, listeners);
            List<CreditBasedBufferQueueView> creditBasedBufferQueueViews = new ArrayList<>();
            for (int index = 0; index < bufferQueues.size(); ++index) {
                creditBasedBufferQueueViews.add(
                        new CreditBasedBufferQueueViewImpl(
                                bufferQueues.get(index),
                                availabilityListener,
                                releaseNotifiers.get(index)));
            }
            return new TieredStoreResultSubpartitionView(
                    subpartitionId,
                    availabilityListener,
                    segmentSearchers,
                    creditBasedBufferQueueViews);
        }
    }

    public void notifyResultSubpartitionViewSendBuffer(
            ResultPartitionID partitionId, int subpartitionId) {
        synchronized (this) {
            Map<Integer, BufferAvailabilityListener> listeners =
                    registeredAvailabilityListeners.get(partitionId);
            if (listeners != null) {
                BufferAvailabilityListener bufferAvailabilityListener =
                        listeners.get(subpartitionId);
                if (bufferAvailabilityListener != null) {
                    bufferAvailabilityListener.notifyDataAvailable();
                }
            }
        }
    }
}
