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

package org.apache.flink.runtime.io.network.partition.tieredstore.upstream.reader;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.StorageTier;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierReaderView;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreConsumer;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreConsumerFailureCause;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkArgument;

/** The reader of Tiered Store. */
public class TieredStoreConsumerImpl implements TieredStoreConsumer {

    private final int subpartitionId;

    private final BufferAvailabilityListener availabilityListener;

    private final StorageTier[] tiers;

    private final TierReaderView[] tierReaderViews;

    private boolean isReleased = false;

    private int currentSegmentId = 0;

    private int consumedSegmentId = 0;

    private boolean hasSegmentFinished = true;

    private int viewIndexContainsCurrentSegment = -1;

    private int currentSequenceNumber = 0;

    public TieredStoreConsumerImpl(
            int subpartitionId,
            BufferAvailabilityListener availabilityListener,
            StorageTier[] tiers)
            throws IOException {
        checkArgument(tiers.length > 0, "The number of StorageTier must be larger than 0.");
        this.subpartitionId = subpartitionId;
        this.availabilityListener = availabilityListener;
        this.tiers = tiers;
        this.tierReaderViews = new TierReaderView[tiers.length];
        createTierReaderViews();
    }

    private void createTierReaderViews() throws IOException {
        for (int i = 0; i < tiers.length; i++) {
            tierReaderViews[i] =
                    tiers[i].createTierReaderView(subpartitionId, availabilityListener);
        }
    }

    @Nullable
    @Override
    public BufferAndBacklog getNextBuffer() throws IOException {
        synchronized (this) {
            if (currentSegmentId <= consumedSegmentId) {
                return getNextBufferInternal();
            }
        }
        return null;
    }

    @Override
    public void updateConsumedSegmentId(int segmentId) {
        synchronized (this) {
            consumedSegmentId = segmentId;
        }
    }

    @Override
    public void forceNotifyAvailable() {
        availabilityListener.notifyDataAvailable();
    }

    public BufferAndBacklog getNextBufferInternal() throws IOException {
        if (!findTierContainsNextSegment()) {
            return null;
        }
        BufferAndBacklog bufferAndBacklog =
                tierReaderViews[viewIndexContainsCurrentSegment].getNextBuffer();

        if (bufferAndBacklog != null) {
            hasSegmentFinished = bufferAndBacklog.isLastBufferInSegment();
            if (hasSegmentFinished) {
                currentSegmentId++;
            }
            if (bufferAndBacklog.buffer() == null) {
                return getNextBuffer();
            }
            bufferAndBacklog.setSequenceNumber(currentSequenceNumber);
            currentSequenceNumber++;
        }
        return bufferAndBacklog;
    }

    @Override
    public ResultSubpartitionView.AvailabilityWithBacklog getAvailabilityAndBacklog(
            int numCreditsAvailable) {
        if (findTierContainsNextSegment()) {
            return tierReaderViews[viewIndexContainsCurrentSegment].getAvailabilityAndBacklog(
                    numCreditsAvailable);
        }
        return new ResultSubpartitionView.AvailabilityWithBacklog(false, 0);
    }

    @Override
    public void releaseAllResources() throws IOException {
        if (isReleased) {
            return;
        }
        isReleased = true;
        for (TierReaderView tierReaderView : tierReaderViews) {
            tierReaderView.releaseAllResources();
        }
    }

    @Override
    public boolean isReleased() {
        return isReleased;
    }

    @Override
    public Throwable getFailureCause() {
        TieredStoreConsumerFailureCause failureCause = new TieredStoreConsumerFailureCause();
        for (TierReaderView tierReaderView : tierReaderViews) {
            failureCause.appendException(tierReaderView.getFailureCause());
        }
        return failureCause.isEmpty() ? null : failureCause;
    }

    @Override
    public int unsynchronizedGetNumberOfQueuedBuffers() {
        findTierContainsNextSegment();
        return tierReaderViews[viewIndexContainsCurrentSegment]
                .unsynchronizedGetNumberOfQueuedBuffers();
    }

    @Override
    public int getNumberOfQueuedBuffers() {
        findTierContainsNextSegment();
        return tierReaderViews[viewIndexContainsCurrentSegment].getNumberOfQueuedBuffers();
    }

    @VisibleForTesting
    public int getCurrentSegmentId() {
        return currentSegmentId;
    }

    // -------------------------------
    //       Internal Methods
    // -------------------------------

    private boolean findTierContainsNextSegment() {
        if (!hasSegmentFinished) {
            return true;
        }
        for (int i = 0; i < tiers.length; i++) {
            StorageTier tieredDataGate = tiers[i];
            if (tieredDataGate.hasCurrentSegment(subpartitionId, currentSegmentId)) {
                viewIndexContainsCurrentSegment = i;
                return true;
            }
        }
        return false;
    }
}
