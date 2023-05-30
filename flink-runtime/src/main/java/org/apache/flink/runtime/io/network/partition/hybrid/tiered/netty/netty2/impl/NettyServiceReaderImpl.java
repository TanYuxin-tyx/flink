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

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.ConsumerNettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.netty2.NettyServiceReader;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;
import java.util.Optional;
import java.util.function.BiConsumer;

/** The default implementation of {@link ConsumerNettyService}. */
public class NettyServiceReaderImpl implements NettyServiceReader {

    private final InputChannel[] inputChannels;
    private final BiConsumer<Integer, Boolean> subpartitionAvailableNotifier;
    private final int[] lastPrioritySequenceNumber;

    public NettyServiceReaderImpl(
            InputChannel[] inputChannels,
            BiConsumer<Integer, Boolean> subpartitionAvailableNotifier,
            int[] lastPrioritySequenceNumber) {
        this.inputChannels = inputChannels;
        this.subpartitionAvailableNotifier = subpartitionAvailableNotifier;
        this.lastPrioritySequenceNumber = lastPrioritySequenceNumber;
    }

    @Override
    public Optional<Buffer> readBuffer(int subpartitionId) {
        Optional<InputChannel.BufferAndAvailability> bufferAndAvailability = Optional.empty();
        try {
            bufferAndAvailability = inputChannels[subpartitionId].getNextBuffer();
        } catch (IOException | InterruptedException e) {
            ExceptionUtils.rethrow(e, "Failed to read buffer in Consumer Netty Service.");
        }
        if (bufferAndAvailability.isPresent()) {
            if (bufferAndAvailability.get().moreAvailable()) {
                notifyResultSubpartitionAvailable(
                        subpartitionId, bufferAndAvailability.get().hasPriority());
            }
            if (bufferAndAvailability.get().hasPriority()) {
                lastPrioritySequenceNumber[subpartitionId] =
                        bufferAndAvailability.get().getSequenceNumber();
            }
        }
        return bufferAndAvailability.map(InputChannel.BufferAndAvailability::buffer);
    }

    @Override
    public void notifyResultSubpartitionAvailable(int subpartitionId, boolean priority) {
        subpartitionAvailableNotifier.accept(subpartitionId, priority);
    }

    @Override
    public void notifyRequiredSegmentId(int subpartitionId, int segmentId) {
        inputChannels[subpartitionId].notifyRequiredSegmentId(segmentId);
    }
}
