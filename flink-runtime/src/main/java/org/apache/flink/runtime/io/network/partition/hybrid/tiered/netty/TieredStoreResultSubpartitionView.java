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
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.shuffle.TieredResultPartition;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.NettyPayload;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Queue;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.END_OF_SEGMENT;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The {@link TieredStoreResultSubpartitionView} is the implementation of {@link
 * ResultSubpartitionView} of {@link TieredResultPartition}.
 */
public class TieredStoreResultSubpartitionView implements ResultSubpartitionView {

    private final BufferAvailabilityListener availabilityListener;

    private final List<Queue<NettyPayload>> bufferQueues;

    private final List<NettyProducerService> producerServices;

    private final List<NettyConnectionId> nettyConnectionIds;

    private boolean isReleased = false;

    private int requiredSegmentId = 0;

    private boolean stopSendingData = false;

    private int queueIndexContainsCurrentSegment = -1;

    private int currentSequenceNumber = -1;

    public TieredStoreResultSubpartitionView(
            BufferAvailabilityListener availabilityListener,
            List<Queue<NettyPayload>> bufferQueues,
            List<NettyConnectionId> nettyConnectionIds,
            List<NettyProducerService> producerServices) {
        this.availabilityListener = availabilityListener;
        this.bufferQueues = bufferQueues;
        this.nettyConnectionIds = nettyConnectionIds;
        this.producerServices = producerServices;
    }

    @Nullable
    @Override
    public BufferAndBacklog getNextBuffer() throws IOException {
        if (stopSendingData || !findCurrentBufferQueue()) {
            return null;
        }
        Queue<NettyPayload> currentQueue = bufferQueues.get(queueIndexContainsCurrentSegment);
        Optional<Buffer> nextBuffer =
                readBufferQueue(
                        currentQueue,
                        producerServices.get(queueIndexContainsCurrentSegment),
                        nettyConnectionIds.get(queueIndexContainsCurrentSegment));
        if (nextBuffer.isPresent()) {
            stopSendingData = nextBuffer.get().getDataType() == END_OF_SEGMENT;
            if (stopSendingData) {
                queueIndexContainsCurrentSegment = -1;
            }
            currentSequenceNumber++;
            return BufferAndBacklog.fromBufferAndLookahead(
                    nextBuffer.get(),
                    getBufferQueueNextDataType(currentQueue),
                    currentQueue.size(),
                    currentSequenceNumber);
        }
        return null;
    }

    @Override
    public AvailabilityWithBacklog getAvailabilityAndBacklog(int numCreditsAvailable) {
        if (findCurrentBufferQueue()) {
            Queue<NettyPayload> currentQueue = bufferQueues.get(queueIndexContainsCurrentSegment);
            boolean availability = numCreditsAvailable > 0;
            if (numCreditsAvailable <= 0
                    && getBufferQueueNextDataType(currentQueue) == Buffer.DataType.EVENT_BUFFER) {
                availability = true;
            }
            return new AvailabilityWithBacklog(availability, currentQueue.size());
        }
        return new AvailabilityWithBacklog(false, 0);
    }

    @Override
    public void notifyRequiredSegmentId(int segmentId) {
        requiredSegmentId = segmentId;
        stopSendingData = false;
        availabilityListener.notifyDataAvailable();
    }

    @Override
    public void releaseAllResources() throws IOException {
        if (isReleased) {
            return;
        }
        isReleased = true;
        for (int index = 0; index < bufferQueues.size(); ++index) {
            releaseQueue(
                    bufferQueues.get(index),
                    producerServices.get(index),
                    nettyConnectionIds.get(index));
        }
    }

    @Override
    public boolean isReleased() {
        return isReleased;
    }

    @Override
    public Throwable getFailureCause() {
        // nothing to do
        return null;
    }

    @Override
    public int unsynchronizedGetNumberOfQueuedBuffers() {
        findCurrentBufferQueue();
        return bufferQueues.get(queueIndexContainsCurrentSegment).size();
    }

    @Override
    public int getNumberOfQueuedBuffers() {
        findCurrentBufferQueue();
        return bufferQueues.get(queueIndexContainsCurrentSegment).size();
    }

    @Override
    public void notifyDataAvailable() {
        throw new UnsupportedOperationException(
                "Method notifyDataAvailable should never be called.");
    }

    @Override
    public void resumeConsumption() {
        throw new UnsupportedOperationException("Method resumeConsumption should never be called.");
    }

    @Override
    public void acknowledgeAllDataProcessed() {
        // nothing to do.
    }

    @Override
    public void notifyNewBufferSize(int newBufferSize) {
        throw new UnsupportedOperationException(
                "Method notifyNewBufferSize should never be called.");
    }

    // -------------------------------
    //       Internal Methods
    // -------------------------------

    private Optional<Buffer> readBufferQueue(
            Queue<NettyPayload> bufferQueue,
            NettyProducerService producerService,
            NettyConnectionId id)
            throws IOException {
        NettyPayload buffer = bufferQueue.poll();
        if (buffer == null) {
            return Optional.empty();
        } else {
            if (buffer.getSegmentId() != -1) {
                return readBufferQueue(bufferQueue, producerService, id);
            }
            Throwable readError = buffer.getError();
            if (readError != null) {
                releaseQueue(bufferQueue, producerService, id);
                throw new IOException(readError);
            } else {
                return Optional.of(checkNotNull(buffer.getBuffer()));
            }
        }
    }

    private Buffer.DataType getBufferQueueNextDataType(Queue<NettyPayload> bufferQueue) {
        NettyPayload nextBuffer = bufferQueue.peek();
        if (nextBuffer == null || nextBuffer.getBuffer() == null) {
            return Buffer.DataType.NONE;
        } else {
            return nextBuffer.getBuffer().getDataType();
        }
    }

    private void releaseQueue(
            Queue<NettyPayload> bufferQueue,
            NettyProducerService producerService,
            NettyConnectionId id) {
        NettyPayload nettyPayload;
        while ((nettyPayload = bufferQueue.poll()) != null) {
            if (nettyPayload.getBuffer() != null) {
                checkNotNull(nettyPayload.getBuffer()).recycleBuffer();
            }
        }
        producerService.disconnectNettyConnection(id);
    }

    private boolean findCurrentBufferQueue() {
        if (queueIndexContainsCurrentSegment != -1 && !stopSendingData) {
            return true;
        }
        for (int queueIndex = 0; queueIndex < bufferQueues.size(); queueIndex++) {
            NettyPayload firstNettyPayload = bufferQueues.get(queueIndex).peek();
            if (firstNettyPayload == null
                    || firstNettyPayload.getSegmentId() != requiredSegmentId) {
                continue;
            }
            queueIndexContainsCurrentSegment = queueIndex;
            return true;
        }
        return false;
    }
}
