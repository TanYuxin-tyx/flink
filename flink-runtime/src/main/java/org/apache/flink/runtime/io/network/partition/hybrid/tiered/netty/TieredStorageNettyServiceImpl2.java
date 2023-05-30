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
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.netty2.NettyServiceReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.netty2.NettyServiceWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.netty2.TieredStorageNettyService2;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.BufferContext;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.BiConsumer;

/**
 * {@link TieredStorageNettyServiceImpl2} is used to create netty services in producer and consumer
 * side.
 */
public class TieredStorageNettyServiceImpl2 implements TieredStorageNettyService2 {
    @Override
    public NettyServiceWriter registerProducer(int subpartition) {
        Queue<BufferContext> bufferQueue = new LinkedBlockingQueue<>();
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
}
