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
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.remote.RemoteTier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkArgument;

/** The reader of Tiered Store. */
public class TieredStoreConsumerImpl implements TieredStoreConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(TieredStoreConsumerImpl.class);

    private final int subpartitionId;

    private final BufferAvailabilityListener availabilityListener;

    private final StorageTier[] tierDataGates;

    private final TierReaderView[] tierReaderViews;

    private boolean isReleased = false;

    private int currentSegmentIndex = 0;

    private long consumedSegmentIndex = 0L;

    private boolean hasSegmentFinished = true;

    private int viewIndexContainsCurrentSegment = -1;

    private int currentSequenceNumber = 0;

    public TieredStoreConsumerImpl(
            int subpartitionId,
            BufferAvailabilityListener availabilityListener,
            StorageTier[] tierDataGates)
            throws IOException {
        checkArgument(tierDataGates.length > 0, "Empty tier transmitters.");

        this.subpartitionId = subpartitionId;
        this.availabilityListener = availabilityListener;
        this.tierDataGates = tierDataGates;
        this.tierReaderViews = new TierReaderView[tierDataGates.length];
        createSingleTierReaders();
    }

    private void createSingleTierReaders() throws IOException {
        for (int i = 0; i < tierDataGates.length; i++) {
            tierReaderViews[i] =
                    tierDataGates[i].createSubpartitionTierReaderView(
                            subpartitionId, availabilityListener);
        }
    }

    @Nullable
    @Override
    public BufferAndBacklog getNextBuffer() throws IOException {
        synchronized (this) {
            if (currentSegmentIndex <= consumedSegmentIndex) {
                return getNextBufferInternal();
            }
        }
        return null;
    }

    @Override
    public void updateConsumedSegmentIndex(int segmentId) {
        synchronized (this) {
            consumedSegmentIndex = segmentId;
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
                currentSegmentIndex++;
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

        // update the needNotify status of all gates
        for (TierReaderView tierReaderView : tierReaderViews) {
            tierReaderView.getAvailabilityAndBacklog(numCreditsAvailable);
        }

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
            if (tierReaderView != null) {
                try {
                    tierReaderView.releaseAllResources();
                } catch (IOException ioException) {
                    throw new RuntimeException(
                            "Failed to release partition view resources.", ioException);
                }
            }
        }
    }

    @Override
    public boolean isReleased() {
        return isReleased;
    }

    @Override
    public Throwable getFailureCause() {
        for (TierReaderView tierReaderView : tierReaderViews) {
            Throwable failureCause = tierReaderView.getFailureCause();
            if (failureCause != null) {
                return failureCause;
            }
        }
        return null;
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

    @Override
    public boolean containSegment(int segmentId) {
        for (StorageTier tieredDataGate : tierDataGates) {
            if (tieredDataGate.getClass() == RemoteTier.class) {
                continue;
            }
            if (tieredDataGate.hasCurrentSegment(subpartitionId, segmentId)) {
                return true;
            }
        }
        return false;
    }

    @VisibleForTesting
    public int getCurrentSegmentIndex() {
        return currentSegmentIndex;
    }

    // -------------------------------
    //       Internal Methods
    // -------------------------------

    private boolean findTierContainsNextSegment() {

        for (TierReaderView tierReaderView : tierReaderViews) {
            tierReaderView.getAvailabilityAndBacklog(Integer.MAX_VALUE);
        }

        if (!hasSegmentFinished) {
            return true;
        }

        for (int i = 0; i < tierDataGates.length; i++) {
            StorageTier tieredDataGate = tierDataGates[i];
            if (tieredDataGate.hasCurrentSegment(subpartitionId, currentSegmentIndex)) {
                viewIndexContainsCurrentSegment = i;
                return true;
            }
        }

        return false;
    }
}
