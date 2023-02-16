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

package org.apache.flink.runtime.io.network.partition.store.reader;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.store.common.SingleTierDataGate;
import org.apache.flink.runtime.io.network.partition.store.common.SingleTierReader;
import org.apache.flink.runtime.io.network.partition.store.common.TieredStoreConsumer;
import org.apache.flink.runtime.io.network.partition.store.tier.remote.DfsDataManager;

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

    private final SingleTierDataGate[] tierDataGates;

    private final SingleTierReader[] singleTierReaders;

    private boolean isReleased = false;

    private int currentSegmentIndex = 0;

    private long consumedSegmentIndex = 0L;

    private boolean hasSegmentFinished = true;

    private int viewIndexContainsCurrentSegment = -1;

    private int currentSequenceNumber = 0;

    private String taskName;

    public TieredStoreConsumerImpl(
            int subpartitionId,
            BufferAvailabilityListener availabilityListener,
            SingleTierDataGate[] tierDataGates,
            String taskName)
            throws IOException {
        checkArgument(tierDataGates.length > 0, "Empty tier transmitters.");

        this.subpartitionId = subpartitionId;
        this.availabilityListener = availabilityListener;
        this.tierDataGates = tierDataGates;
        this.singleTierReaders = new SingleTierReader[tierDataGates.length];
        createSingleTierReaders();
        this.taskName = taskName;
    }

    private void createSingleTierReaders() throws IOException {
        for (int i = 0; i < tierDataGates.length; i++) {
            singleTierReaders[i] =
                    tierDataGates[i].createSubpartitionTierReader(
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
    public void updateConsumedSegmentIndex(long segmentId) {
        synchronized (this) {
            consumedSegmentIndex = segmentId;
        }
    }

    @Override
    public void forceNotifyAvailable() {
        availabilityListener.notifyDataAvailable();
    }

    public BufferAndBacklog getNextBufferInternal() throws IOException {
        LOG.debug("%%% getNextBuffer1");
        if (!findTierContainsNextSegment()) {
            return null;
        }
        LOG.debug("%%% getNextBuffer2");
        BufferAndBacklog bufferAndBacklog =
                singleTierReaders[viewIndexContainsCurrentSegment].getNextBuffer();

        if (bufferAndBacklog != null) {
            LOG.debug("%%% getNextBuffer3 ");
            bufferAndBacklog.setNextDataType(Buffer.DataType.DATA_BUFFER);
            hasSegmentFinished = bufferAndBacklog.isLastBufferInSegment();
            if (hasSegmentFinished) {
                Buffer buffer = bufferAndBacklog.buffer();
                if (buffer != null && buffer.getDataType() == Buffer.DataType.DATA_BUFFER) {
                    System.out.println();
                }
                currentSegmentIndex++;
            }
            if (bufferAndBacklog.isFromDfsTier()) {
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
        // first scan all result subpartition views
        for (SingleTierReader singleTierReader : singleTierReaders) {
            singleTierReader.getAvailabilityAndBacklog(numCreditsAvailable);
        }
        if (findTierContainsNextSegment()) {
            return singleTierReaders[viewIndexContainsCurrentSegment].getAvailabilityAndBacklog(
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
        for (int i = 0; i < singleTierReaders.length; i++) {
            if (singleTierReaders[i] != null) {
                try {
                    singleTierReaders[i].releaseAllResources();
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
        for (SingleTierReader singleTierReader : singleTierReaders) {
            Throwable failureCause = singleTierReader.getFailureCause();
            if (failureCause != null) {
                return failureCause;
            }
        }
        return null;
    }

    @Override
    public int unsynchronizedGetNumberOfQueuedBuffers() {
        findTierContainsNextSegment();
        return singleTierReaders[viewIndexContainsCurrentSegment]
                .unsynchronizedGetNumberOfQueuedBuffers();
    }

    @Override
    public int getNumberOfQueuedBuffers() {
        findTierContainsNextSegment();
        return singleTierReaders[viewIndexContainsCurrentSegment].getNumberOfQueuedBuffers();
    }

    @Override
    public boolean containSegment(long segmentId) {
        for (SingleTierDataGate tieredDataGate : tierDataGates) {
            if (tieredDataGate.getClass() == DfsDataManager.class) {
                continue;
            }
            if (tieredDataGate.hasCurrentSegment(subpartitionId, segmentId)) {
                return true;
            }
        }
        return false;
    }

    // -------------------------------
    //       Internal Methods
    // -------------------------------

    private boolean findTierContainsNextSegment() {

        for (SingleTierReader singleTierReader : singleTierReaders) {
            singleTierReader.getAvailabilityAndBacklog(Integer.MAX_VALUE);
        }

        if (!hasSegmentFinished) {
            return true;
        }

        for (int i = 0; i < tierDataGates.length; i++) {
            SingleTierDataGate tieredDataGate = tierDataGates[i];
            if (tieredDataGate.hasCurrentSegment(subpartitionId, currentSegmentIndex)) {
                viewIndexContainsCurrentSegment = i;
                return true;
            }
        }

        return false;
    }
}
