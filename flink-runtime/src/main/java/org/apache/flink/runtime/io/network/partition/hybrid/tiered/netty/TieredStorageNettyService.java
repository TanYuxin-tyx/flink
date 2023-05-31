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

import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;

import java.util.function.BiConsumer;

/** {@link TieredStorageNettyService} is used to create writers and readers to netty. */
public interface TieredStorageNettyService {

    /**
     * Register to {@link TieredStorageNettyService} and create a {@link NettyServiceWriter).
     * @param writerId writer id is used as the unique id of writer.
     * @param serviceReleaseNotifier notifier is used to notify that the service is released.
     * @return the writer.
     */
    NettyServiceWriter registerProducer(
            NettyServiceWriterId writerId, Runnable serviceReleaseNotifier);

    /**
     * Register to {@link TieredStorageNettyService} and create a {@link NettyServiceReader).
     * @param inputChannels channels in consumer side.
     * @param lastPrioritySequenceNumber the array to record the priority sequence number.
     * @param subpartitionAvailableNotifier notifier is to notify the subpartition is available.
     * @return the reader.
     */
    NettyServiceReader registerConsumer(
            InputChannel[] inputChannels,
            BiConsumer<Integer, Boolean> subpartitionAvailableNotifier,
            int[] lastPrioritySequenceNumber);
}
