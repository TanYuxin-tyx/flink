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
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;
import java.util.Optional;
import java.util.function.BiConsumer;

/** The default implementation of {@link NettyConnectionReader}. */
public class NettyConnectionReaderImpl implements NettyConnectionReader {

    /** subpartitionId is used to indicate the id of subpartition. */
    private final int subpartitionId;

    /**
     * inputChannels is initialized in {@link SingleInputGate} and used to read buffer from netty
     * connection.
     */
    private final InputChannel[] inputChannels;

    /**
     * queueChannelCallback is used to queue a channel to {@link SingleInputGate} to trigger next
     * round of reading from this channel. The Integer in the return type is the index of input
     * channel and the Boolean in the return type is whether to queue the channel with priority.
     */
    private final BiConsumer<Integer, Boolean> queueChannelCallback;

    /**
     * The array is used to record the latest sequence number of buffer with priority data type,
     * which can decide outdated status of sequence number and whether to enqueue the related input
     * channel to {@link SingleInputGate}.
     */
    private final int[] lastPrioritySequenceNumber;

    /** The last required segment id. */
    private int lastRequiredSegmentId = 0;

    public NettyConnectionReaderImpl(
            int subpartitionId,
            InputChannel[] inputChannels,
            BiConsumer<Integer, Boolean> queueChannelCallback,
            int[] lastPrioritySequenceNumber) {
        this.subpartitionId = subpartitionId;
        this.inputChannels = inputChannels;
        this.queueChannelCallback = queueChannelCallback;
        this.lastPrioritySequenceNumber = lastPrioritySequenceNumber;
    }

    @Override
    public Optional<Buffer> readBuffer(int segmentId) {
        if (segmentId > 0L && (segmentId != lastRequiredSegmentId)) {
            lastRequiredSegmentId = segmentId;
            inputChannels[subpartitionId].notifyRequiredSegmentId(segmentId);
        }
        Optional<InputChannel.BufferAndAvailability> bufferAndAvailability = Optional.empty();
        try {
            bufferAndAvailability = inputChannels[subpartitionId].getNextBuffer();
        } catch (IOException | InterruptedException e) {
            ExceptionUtils.rethrow(e, "Failed to read buffer.");
        }
        if (bufferAndAvailability.isPresent()) {
            if (bufferAndAvailability.get().moreAvailable()) {
                queueChannelCallback.accept(
                        inputChannels[subpartitionId].getChannelIndex(),
                        bufferAndAvailability.get().hasPriority());
            }
            if (bufferAndAvailability.get().hasPriority()) {
                lastPrioritySequenceNumber[inputChannels[subpartitionId].getChannelIndex()] =
                        bufferAndAvailability.get().getSequenceNumber();
            }
        }
        return bufferAndAvailability.map(InputChannel.BufferAndAvailability::buffer);
    }
}
