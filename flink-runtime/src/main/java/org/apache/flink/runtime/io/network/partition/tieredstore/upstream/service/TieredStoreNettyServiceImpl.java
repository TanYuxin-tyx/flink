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

package org.apache.flink.runtime.io.network.partition.tieredstore.upstream.service;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.StorageTier;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierReaderView;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreConsumerFailureCause;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.SEGMENT_EVENT;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * The {@link TieredStoreNettyServiceImpl} is the implementation of {@link TieredStoreNettyService}.
 */
public class TieredStoreNettyServiceImpl implements TieredStoreNettyService {

    private final BufferAvailabilityListener availabilityListener;

    private final StorageTier[] allTiers;

    private final int subpartitionId;

    private List<StorageTier> registeredTiers;

    private List<TierReaderView> registeredTierReaderViews;

    private boolean isClosed = false;

    private int requiredSegmentId = 0;

    private boolean stopSendingData = false;

    private int viewIndexContainsCurrentSegment = -1;

    private int currentSequenceNumber = 0;

    public TieredStoreNettyServiceImpl(
            int subpartitionId,
            BufferAvailabilityListener availabilityListener,
            StorageTier[] allTiers)
            throws IOException {
        checkArgument(allTiers.length > 0, "The number of StorageTier must be larger than 0.");
        this.allTiers = allTiers;
        this.subpartitionId = subpartitionId;
        this.availabilityListener = availabilityListener;
    }

    public void start() throws IOException {
        registeredTiers = new ArrayList<>();
        registeredTierReaderViews = new ArrayList<>();
        for (StorageTier tier : allTiers) {
            TierReaderView tierReaderView =
                    tier.createTierReaderView(subpartitionId, availabilityListener);
            if (tierReaderView != null) {
                registeredTiers.add(tier);
                registeredTierReaderViews.add(tierReaderView);
            }
        }
    }

    @Nullable
    @Override
    public BufferAndBacklog getNextBuffer() throws IOException {
        if (stopSendingData || !findTierReaderViewIndex()) {
            return null;
        }
        BufferAndBacklog bufferAndBacklog =
                registeredTierReaderViews.get(viewIndexContainsCurrentSegment).getNextBuffer();
        if (bufferAndBacklog != null) {
            stopSendingData = bufferAndBacklog.buffer().getDataType() == SEGMENT_EVENT;
            bufferAndBacklog.setSequenceNumber(currentSequenceNumber);
            currentSequenceNumber++;
        }
        return bufferAndBacklog;
    }

    @Override
    public void updateRequiredSegmentId(int segmentId) {
        requiredSegmentId = segmentId;
        stopSendingData = false;
    }

    @Override
    public void forceNotifyAvailable() {
        availabilityListener.notifyDataAvailable();
    }

    @Override
    public ResultSubpartitionView.AvailabilityWithBacklog getAvailabilityAndBacklog(
            int numCreditsAvailable) {
        if (findTierReaderViewIndex()) {
            return registeredTierReaderViews
                    .get(viewIndexContainsCurrentSegment)
                    .getAvailabilityAndBacklog(numCreditsAvailable);
        }
        return new ResultSubpartitionView.AvailabilityWithBacklog(false, 0);
    }

    @Override
    public void close() throws IOException {
        if (isClosed) {
            return;
        }
        isClosed = true;
        for (TierReaderView tierReaderView : registeredTierReaderViews) {
            tierReaderView.release();
        }
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public Throwable getFailureCause() {
        TieredStoreConsumerFailureCause failureCause = new TieredStoreConsumerFailureCause();
        for (TierReaderView tierReaderView : registeredTierReaderViews) {
            failureCause.appendException(tierReaderView.getFailureCause());
        }
        return failureCause.isEmpty() ? null : failureCause;
    }

    @Override
    public int unsynchronizedGetNumberOfQueuedBuffers() {
        findTierReaderViewIndex();
        return registeredTierReaderViews
                .get(viewIndexContainsCurrentSegment)
                .unsynchronizedGetNumberOfQueuedBuffers();
    }

    @Override
    public int getNumberOfQueuedBuffers() {
        findTierReaderViewIndex();
        return registeredTierReaderViews
                .get(viewIndexContainsCurrentSegment)
                .getNumberOfQueuedBuffers();
    }

    @VisibleForTesting
    public int getRequiredSegmentId() {
        return requiredSegmentId;
    }

    // -------------------------------
    //       Internal Methods
    // -------------------------------

    private boolean findTierReaderViewIndex() {
        for (TierReaderView tierReaderView : registeredTierReaderViews) {
            tierReaderView.updateNeedNotifyStatus();
        }
        for (int i = 0; i < registeredTiers.size(); i++) {
            StorageTier tieredDataGate = registeredTiers.get(i);
            if (tieredDataGate.hasCurrentSegment(subpartitionId, requiredSegmentId)) {
                viewIndexContainsCurrentSegment = i;
                return true;
            }
        }
        return false;
    }
}
