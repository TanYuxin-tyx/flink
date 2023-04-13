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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.service;

import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.TieredStoreConsumerFailureCause;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.ADD_SEGMENT_ID_EVENT;

/**
 * The {@link TieredStoreResultSubpartitionView} is the implementation of {@link
 * TieredStoreNettyService}.
 */
public class TieredStoreResultSubpartitionView implements ResultSubpartitionView {

    private final BufferAvailabilityListener availabilityListener;

    private final int subpartitionId;

    private final List<NettyBasedTierConsumerViewProvider> registeredTiers;

    private final List<NettyBasedTierConsumerView> registeredTierConsumerViews;

    private boolean isReleased = false;

    private int requiredSegmentId = 0;

    private boolean stopSendingData = false;

    private int viewIndexContainsCurrentSegment = -1;

    private int currentSequenceNumber = 0;

    public TieredStoreResultSubpartitionView(
            int subpartitionId,
            BufferAvailabilityListener availabilityListener,
            List<NettyBasedTierConsumerViewProvider> registeredTiers,
            List<NettyBasedTierConsumerView> registeredTierConsumerViews) {
        this.subpartitionId = subpartitionId;
        this.availabilityListener = availabilityListener;
        this.registeredTiers = registeredTiers;
        this.registeredTierConsumerViews = registeredTierConsumerViews;
    }

    @Nullable
    @Override
    public BufferAndBacklog getNextBuffer() throws IOException {
        if (stopSendingData || !findTierReaderViewIndex()) {
            return null;
        }
        BufferAndBacklog bufferAndBacklog =
                registeredTierConsumerViews.get(viewIndexContainsCurrentSegment).getNextBuffer();
        if (bufferAndBacklog != null) {
            stopSendingData = bufferAndBacklog.buffer().getDataType() == ADD_SEGMENT_ID_EVENT;
            bufferAndBacklog.setSequenceNumber(currentSequenceNumber);
            currentSequenceNumber++;
        }
        return bufferAndBacklog;
    }

    @Override
    public ResultSubpartitionView.AvailabilityWithBacklog getAvailabilityAndBacklog(
            int numCreditsAvailable) {
        if (findTierReaderViewIndex()) {
            return registeredTierConsumerViews
                    .get(viewIndexContainsCurrentSegment)
                    .getAvailabilityAndBacklog(numCreditsAvailable);
        }
        return new ResultSubpartitionView.AvailabilityWithBacklog(false, 0);
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
        for (NettyBasedTierConsumerView nettyBasedTierConsumerView : registeredTierConsumerViews) {
            nettyBasedTierConsumerView.release();
        }
        registeredTierConsumerViews.clear();
        registeredTiers.clear();
    }

    @Override
    public boolean isReleased() {
        return isReleased;
    }

    @Override
    public Throwable getFailureCause() {
        TieredStoreConsumerFailureCause failureCause = new TieredStoreConsumerFailureCause();
        for (NettyBasedTierConsumerView nettyBasedTierConsumerView : registeredTierConsumerViews) {
            failureCause.appendException(nettyBasedTierConsumerView.getFailureCause());
        }
        return failureCause.isEmpty() ? null : failureCause;
    }

    @Override
    public int unsynchronizedGetNumberOfQueuedBuffers() {
        findTierReaderViewIndex();
        return registeredTierConsumerViews
                .get(viewIndexContainsCurrentSegment)
                .unsynchronizedGetNumberOfQueuedBuffers();
    }

    @Override
    public int getNumberOfQueuedBuffers() {
        findTierReaderViewIndex();
        return registeredTierConsumerViews
                .get(viewIndexContainsCurrentSegment)
                .getNumberOfQueuedBuffers();
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
        for (NettyBasedTierConsumerView nettyBasedTierConsumerView : registeredTierConsumerViews) {
            nettyBasedTierConsumerView.updateNeedNotifyStatus();
        }
        for (int viewIndex = 0; viewIndex < registeredTiers.size(); viewIndex++) {
            NettyBasedTierConsumerViewProvider tierConsumerViewProvider =
                    registeredTiers.get(viewIndex);
            if (tierConsumerViewProvider.hasCurrentSegment(subpartitionId, requiredSegmentId)) {
                viewIndexContainsCurrentSegment = viewIndex;
                return true;
            }
        }
        return false;
    }
}
