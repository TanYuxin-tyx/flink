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

package org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.memory;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierReader;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierReaderView;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** The read view of {@link MemoryTierReader}.*/
public class MemoryTierReaderView implements TierReaderView {

    private final Object lock = new Object();

    private final BufferAvailabilityListener availabilityListener;

    @GuardedBy("lock")
    private int lastConsumedBufferIndex = -1;

    @GuardedBy("lock")
    private boolean needNotify = true;

    @Nullable
    @GuardedBy("lock")
    private Buffer.DataType cachedNextDataType = null;

    @Nullable
    @GuardedBy("lock")
    private Throwable failureCause = null;

    @GuardedBy("lock")
    private boolean isReleased = false;

    @Nullable
    @GuardedBy("lock")
    private TierReader memoryTierReader;

    public MemoryTierReaderView(BufferAvailabilityListener availabilityListener) {
        this.availabilityListener = availabilityListener;
    }

    @Nullable
    @Override
    public BufferAndBacklog getNextBuffer() throws IOException {
        try {
            synchronized (lock) {
                checkNotNull(memoryTierReader, "memory data view must be not null.");
                Optional<BufferAndBacklog> bufferToConsume =
                        memoryTierReader.consumeBuffer(lastConsumedBufferIndex + 1, null);
                updateConsumingStatus(bufferToConsume);
                return bufferToConsume.map(this::handleBacklog).orElse(null);
            }
        } catch (Throwable cause) {
            releaseInternal(cause);
            throw new IOException("Failed to get next buffer.", cause);
        }
    }

    @Override
    public void notifyDataAvailable() {
        boolean notifyDownStream = false;
        synchronized (lock) {
            if (isReleased) {
                return;
            }
            if (needNotify) {
                notifyDownStream = true;
                needNotify = false;
            }
        }
        // notify outside of lock to avoid deadlock
        if (notifyDownStream) {
            availabilityListener.notifyDataAvailable();
        }
    }

    @Override
    public ResultSubpartitionView.AvailabilityWithBacklog getAvailabilityAndBacklog(
            int numCreditsAvailable) {
        synchronized (lock) {
            boolean availability = numCreditsAvailable > 0;
            if (numCreditsAvailable <= 0
                    && cachedNextDataType != null
                    && cachedNextDataType == Buffer.DataType.EVENT_BUFFER) {
                availability = true;
            }
            int backlog = getSubpartitionBacklog();
            if (backlog == 0) {
                needNotify = true;
            }
            return new ResultSubpartitionView.AvailabilityWithBacklog(availability, backlog);
        }
    }

    @Override
    public void releaseAllResources() throws IOException {
        releaseInternal(null);
    }

    @Override
    public boolean isReleased() {
        synchronized (lock) {
            return isReleased;
        }
    }

    @Override
    public Throwable getFailureCause() {
        synchronized (lock) {
            return failureCause;
        }
    }

    public void setMemoryTierReader(TierReader memoryTierReader) {
        synchronized (lock) {
            checkState(
                    this.memoryTierReader == null,
                    "repeatedly set memory data view is not allowed.");
            this.memoryTierReader = memoryTierReader;
        }
    }

    @Override
    public int unsynchronizedGetNumberOfQueuedBuffers() {
        return getSubpartitionBacklog();
    }

    @Override
    public int getNumberOfQueuedBuffers() {
        synchronized (lock) {
            return getSubpartitionBacklog();
        }
    }

    // -------------------------------
    //       Internal Methods
    // -------------------------------

    @SuppressWarnings("FieldAccessNotGuarded")
    private int getSubpartitionBacklog() {
        if (memoryTierReader == null) {
            return 0;
        }
        return memoryTierReader.getBacklog();
    }

    private BufferAndBacklog handleBacklog(BufferAndBacklog bufferToConsume) {
        return bufferToConsume.buffersInBacklog() == 0
                ? new BufferAndBacklog(
                        bufferToConsume.buffer(),
                        getSubpartitionBacklog(),
                        bufferToConsume.getNextDataType(),
                        bufferToConsume.getSequenceNumber(),
                        bufferToConsume.isLastBufferInSegment())
                : bufferToConsume;
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    @GuardedBy("lock")
    private void updateConsumingStatus(Optional<BufferAndBacklog> bufferAndBacklog) {
        assert Thread.holdsLock(lock);
        // if consumed, update and check consume offset
        if (bufferAndBacklog.isPresent()) {
            ++lastConsumedBufferIndex;
            checkState(bufferAndBacklog.get().getSequenceNumber() == lastConsumedBufferIndex);
        }
        // update need-notify
        boolean dataAvailable =
                bufferAndBacklog.map(BufferAndBacklog::isDataAvailable).orElse(false);
        needNotify = !dataAvailable;
        // update cached next data type
        cachedNextDataType = bufferAndBacklog.map(BufferAndBacklog::getNextDataType).orElse(null);
    }

    private void releaseInternal(@Nullable Throwable throwable) {
        boolean releaseMemoryView;
        synchronized (lock) {
            if (isReleased) {
                return;
            }
            isReleased = true;
            failureCause = throwable;
            releaseMemoryView = memoryTierReader != null;
        }
        if (releaseMemoryView) {
            memoryTierReader.releaseDataView();
        }
    }
}
