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
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.SegmentSearcher;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.END_OF_SEGMENT;
import static org.apache.flink.util.Preconditions.checkState;

/** The {@link TieredStoreResultSubpartitionView} is the implementation. */
public class TieredStoreResultSubpartitionView implements ResultSubpartitionView {

    private final BufferAvailabilityListener availabilityListener;

    private final int subpartitionId;

    private final List<SegmentSearcher> segmentSearchers;

    private final List<NettyServiceView> nettyServiceViews;

    private boolean isReleased = false;

    private int requiredSegmentId = 0;

    private boolean stopSendingData = false;

    private int viewIndexContainsCurrentSegment = -1;

    private int currentSequenceNumber = -1;

    public TieredStoreResultSubpartitionView(
            int subpartitionId,
            BufferAvailabilityListener availabilityListener,
            List<SegmentSearcher> segmentSearchers,
            List<NettyServiceView> nettyServiceViews) {
        checkState(segmentSearchers.size() == nettyServiceViews.size());
        this.subpartitionId = subpartitionId;
        this.availabilityListener = availabilityListener;
        this.segmentSearchers = segmentSearchers;
        this.nettyServiceViews = nettyServiceViews;
    }

    @Nullable
    @Override
    public BufferAndBacklog getNextBuffer() throws IOException {
        if (stopSendingData || !findTierReaderViewIndex()) {
            return null;
        }
        NettyServiceView currentNettyServiceView =
                nettyServiceViews.get(viewIndexContainsCurrentSegment);
        Optional<Buffer> nextBuffer = currentNettyServiceView.getNextBuffer();
        if (nextBuffer.isPresent()) {
            stopSendingData = nextBuffer.get().getDataType() == END_OF_SEGMENT;
            currentSequenceNumber++;
            return BufferAndBacklog.fromBufferAndLookahead(
                    nextBuffer.get(),
                    currentNettyServiceView.getNextBufferDataType(),
                    currentNettyServiceView.getNumberOfQueuedBuffers(),
                    currentSequenceNumber);
        }
        return null;
    }

    @Override
    public AvailabilityWithBacklog getAvailabilityAndBacklog(int numCreditsAvailable) {
        if (findTierReaderViewIndex()) {
            return nettyServiceViews
                    .get(viewIndexContainsCurrentSegment)
                    .getAvailabilityAndBacklog(numCreditsAvailable);
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
        for (NettyServiceView nettyServiceView : nettyServiceViews) {
            nettyServiceView.release();
        }
        nettyServiceViews.clear();
        segmentSearchers.clear();
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
        findTierReaderViewIndex();
        return nettyServiceViews.get(viewIndexContainsCurrentSegment).getNumberOfQueuedBuffers();
    }

    @Override
    public int getNumberOfQueuedBuffers() {
        findTierReaderViewIndex();
        return nettyServiceViews.get(viewIndexContainsCurrentSegment).getNumberOfQueuedBuffers();
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

    private boolean findTierReaderViewIndex() {
        for (int viewIndex = 0; viewIndex < segmentSearchers.size(); viewIndex++) {
            SegmentSearcher segmentSearcher = segmentSearchers.get(viewIndex);
            if (segmentSearcher.hasCurrentSegment(subpartitionId, requiredSegmentId)) {
                viewIndexContainsCurrentSegment = viewIndex;
                return true;
            }
        }
        return false;
    }
}
