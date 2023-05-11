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

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.BufferContext;

import java.util.Optional;
import java.util.Queue;
import java.util.function.BiConsumer;

/**
 * The {@link NettyService} is used to provide the netty-based network services in the shuffle
 * process of tiered-store.
 */
public interface NettyService {

    // ------------------------------------
    //        For Producer Side
    // ------------------------------------

    /**
     * Register a buffer queue to the NettyService and the buffer will be consumed from the queue
     * and sent to netty.
     *
     * @param bufferQueue is the required queue.
     * @param availabilityListener is used to listen the available status of buffer queue.
     * @param serviceReleaseNotifier is used to notify that the service is released.
     * @return the view of netty service.
     */
    NettyServiceView register(
            Queue<BufferContext> bufferQueue,
            BufferAvailabilityListener availabilityListener,
            Runnable serviceReleaseNotifier);

    // ------------------------------------
    //        For Consumer Side
    // ------------------------------------

    /**
     * Set up the netty service in consumer side.
     *
     * @param inputChannels in consumer side.
     * @param lastPrioritySequenceNumber is the array to record the priority sequence number.
     * @param subpartitionAvailableNotifier is used to notify the subpartition is available.
     */
    void setup(
            InputChannel[] inputChannels,
            int[] lastPrioritySequenceNumber,
            BiConsumer<Integer, Boolean> subpartitionAvailableNotifier);

    /**
     * Read a buffer related to the specific subpartition from NettyService.
     *
     * @param subpartitionId indicate the subpartition.
     * @return a buffer.
     */
    Optional<Buffer> readBuffer(int subpartitionId);

    /**
     * Notify that the data responding to a subpartition is available.
     *
     * @param subpartitionId indicate the subpartition.
     * @param priority indicate that if the subpartition is priority.
     */
    void notifyResultSubpartitionAvailable(int subpartitionId, boolean priority);

    /**
     * Notify that the specific segment is required according to the subpartitionId and segmentId.
     *
     * @param subpartitionId indicate the subpartition.
     * @param segmentId indicate the id of segment.
     */
    void notifyRequiredSegmentId(int subpartitionId, int segmentId);
}
