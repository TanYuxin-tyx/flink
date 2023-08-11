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

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.END_OF_SEGMENT;
import static org.apache.flink.shaded.curator5.com.google.common.base.Preconditions.checkState;

/**
 * The {@link TieredStorageResultSubpartitionView} is the implementation of {@link
 * ResultSubpartitionView} of {@link TieredResultPartition}.
 */
public class TieredStorageResultSubpartitionView implements ResultSubpartitionView {

    private final BufferAvailabilityListener availabilityListener;

    private final List<NettyPayloadQueue> nettyPayloadQueues;

    private final List<NettyServiceProducer> serviceProducers;

    private final List<NettyConnectionId> nettyConnectionIds;

    private volatile boolean isReleased = false;

    private int requiredSegmentId = 0;

    private boolean stopSendingData = false;

    private int queueIndexContainsCurrentSegment = -1;

    private int currentSequenceNumber = -1;

    public TieredStorageResultSubpartitionView(
            BufferAvailabilityListener availabilityListener,
            List<NettyPayloadQueue> nettyPayloadQueues,
            List<NettyConnectionId> nettyConnectionIds,
            List<NettyServiceProducer> serviceProducers) {
        this.availabilityListener = availabilityListener;
        this.nettyPayloadQueues = nettyPayloadQueues;
        this.nettyConnectionIds = nettyConnectionIds;
        this.serviceProducers = serviceProducers;
    }

    @Nullable
    @Override
    public BufferAndBacklog getNextBuffer() throws IOException {
        if (stopSendingData || !findCurrentNettyPayloadQueue()) {
            return null;
        }
        NettyPayloadQueue currentQueue = nettyPayloadQueues.get(queueIndexContainsCurrentSegment);
        Optional<Buffer> nextBuffer = readNettyPayload(currentQueue);
        if (nextBuffer.isPresent()) {
            stopSendingData = nextBuffer.get().getDataType() == END_OF_SEGMENT;
            if (stopSendingData) {
                queueIndexContainsCurrentSegment = -1;
            }
            currentSequenceNumber++;
            return BufferAndBacklog.fromBufferAndLookahead(
                    nextBuffer.get(),
                    getDataTypeFromNettyPayload(currentQueue.peek()),
                    getNettyBacklog(),
                    currentSequenceNumber);
        }
        return null;
    }

    @Override
    public AvailabilityWithBacklog getAvailabilityAndBacklog(int numCreditsAvailable) {
        checkState(numCreditsAvailable >= 0);
        boolean availability = numCreditsAvailable > 0;
        if (numCreditsAvailable == 0 && shouldIgnoreZeroCredits()) {
            availability = true;
        }
        return new AvailabilityWithBacklog(availability, getNettyBacklog());
    }

    @Override
    public void notifyRequiredSegmentId(int segmentId) {
        if (segmentId > requiredSegmentId) {
            requiredSegmentId = segmentId;
            stopSendingData = false;
            availabilityListener.notifyDataAvailable();
        }
    }

    @Override
    public void releaseAllResources() throws IOException {
        if (isReleased) {
            return;
        }
        isReleased = true;
        for (int index = 0; index < nettyPayloadQueues.size(); ++index) {
            releaseQueue(
                    nettyPayloadQueues.get(index),
                    serviceProducers.get(index),
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
        return getNettyBacklog();
    }

    @Override
    public int getNumberOfQueuedBuffers() {
        return getNettyBacklog();
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

    private Optional<Buffer> readNettyPayload(NettyPayloadQueue nettyPayloadQueue)
            throws IOException {
        NettyPayload nettyPayload = nettyPayloadQueue.poll();
        if (nettyPayload == null) {
            return Optional.empty();
        } else {
            if (nettyPayload.getSegmentId() != -1) {
                return readNettyPayload(nettyPayloadQueue);
            }
            Optional<Throwable> error = nettyPayload.getError();
            if (error.isPresent()) {
                releaseAllResources();
                throw new IOException(error.get());
            } else {
                return nettyPayload.getBuffer();
            }
        }
    }

    private boolean shouldIgnoreZeroCredits() {
        if (findCurrentNettyPayloadQueue()) {
            NettyPayload nettyPayload =
                    nettyPayloadQueues.get(queueIndexContainsCurrentSegment).peek();
            return getDataTypeFromNettyPayload(nettyPayload) == Buffer.DataType.EVENT_BUFFER
                    || getErrorFromNettyPayload(nettyPayload).isPresent();
        }
        return false;
    }

    private Buffer.DataType getDataTypeFromNettyPayload(NettyPayload nettyPayload) {
        if (nettyPayload == null || !nettyPayload.getBuffer().isPresent()) {
            return Buffer.DataType.NONE;
        } else {
            return nettyPayload.getBuffer().get().getDataType();
        }
    }

    private Optional<Throwable> getErrorFromNettyPayload(NettyPayload nettyPayload) {
        return nettyPayload == null ? Optional.empty() : nettyPayload.getError();
    }

    private int getNettyBacklog() {
        int backlog = 0;
        for (NettyPayloadQueue queue : nettyPayloadQueues) {
            backlog += queue.getBacklog();
        }
        return backlog;
    }

    private void releaseQueue(
            NettyPayloadQueue nettyPayloadQueue,
            NettyServiceProducer serviceProducer,
            NettyConnectionId id) {
        NettyPayload nettyPayload;
        while ((nettyPayload = nettyPayloadQueue.poll()) != null) {
            nettyPayload.getBuffer().ifPresent(Buffer::recycleBuffer);
        }
        serviceProducer.connectionBroken(id);
    }

    private boolean findCurrentNettyPayloadQueue() {
        if (queueIndexContainsCurrentSegment != -1 && !stopSendingData) {
            return true;
        }
        for (int queueIndex = 0; queueIndex < nettyPayloadQueues.size(); queueIndex++) {
            NettyPayload firstNettyPayload = nettyPayloadQueues.get(queueIndex).peek();
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
