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
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.BufferContext;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;
import java.util.Optional;
import java.util.Queue;
import java.util.function.BiConsumer;

/** The implementation of {@link NettyService} in consumer side. */
public class ConsumerNettyService implements NettyService {

    private final InputChannel[] inputChannels;

    private final BiConsumer<Integer, Boolean> subpartitionAvailableNotifier;

    private final int[] lastPrioritySequenceNumber;

    public ConsumerNettyService(
            InputChannel[] inputChannels,
            int[] lastPrioritySequenceNumber,
            BiConsumer<Integer, Boolean> subpartitionAvailableNotifier) {
        this.inputChannels = inputChannels;
        this.lastPrioritySequenceNumber = lastPrioritySequenceNumber;
        this.subpartitionAvailableNotifier = subpartitionAvailableNotifier;
    }

    @Override
    public Optional<Buffer> readBuffer(int subpartitionId) {
        Optional<BufferAndAvailability> bufferAndAvailability = Optional.empty();
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
        return bufferAndAvailability.map(BufferAndAvailability::buffer);
    }

    @Override
    public void notifyResultSubpartitionAvailable(int subpartitionId, boolean priority) {
        subpartitionAvailableNotifier.accept(subpartitionId, priority);
    }

    @Override
    public void notifyRequiredSegmentId(int subpartitionId, int segmentId) {
        inputChannels[subpartitionId].notifyRequiredSegmentId(segmentId);
    }

    @Override
    public NettyServiceView register(
            Queue<BufferContext> bufferQueue,
            BufferAvailabilityListener availabilityListener,
            Runnable releaseNotifier) {
        throw new UnsupportedOperationException("Not supported in consumer side.");
    }
}
