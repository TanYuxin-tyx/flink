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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.service;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** The implementation of {@link NettyServiceView}. */
public class NettyServiceViewImpl implements NettyServiceView {

    private final Object viewLock = new Object();

    private final BufferAvailabilityListener availabilityListener;

    @GuardedBy("viewLock")
    private int consumingOffset = -1;

    @GuardedBy("viewLock")
    private boolean needNotify = true;

    @Nullable
    @GuardedBy("viewLock")
    private Buffer.DataType cachedNextDataType = null;

    @Nullable
    @GuardedBy("viewLock")
    private Throwable failureCause = null;

    @GuardedBy("viewLock")
    private boolean isReleased = false;

    @Nullable
    @GuardedBy("viewLock")
    private NettyBufferQueue nettyBufferQueue;

    public NettyServiceViewImpl(BufferAvailabilityListener availabilityListener) {
        this.availabilityListener = availabilityListener;
    }

    @Override
    public void setNettyBufferQueue(NettyBufferQueue nettyBufferQueue) {
        synchronized (viewLock) {
            checkState(
                    this.nettyBufferQueue == null,
                    "Repeatedly set netty buffer queue is not allowed.");
            this.nettyBufferQueue = nettyBufferQueue;
        }
    }

    @Nullable
    @Override
    public BufferAndBacklog getNextBuffer() throws IOException {
        try {
            synchronized (viewLock) {
                checkNotNull(nettyBufferQueue, "Consumer must be not null.");
                Optional<BufferAndBacklog> bufferToConsume =
                        nettyBufferQueue.getNextBuffer(consumingOffset + 1);
                updateConsumingStatus(bufferToConsume);
                return bufferToConsume.orElse(null);
            }
        } catch (Throwable cause) {
            releaseInternal(cause);
            throw new IOException("Failed to get next buffer.", cause);
        }
    }

    @Override
    public void notifyDataAvailable() {
        boolean notifyDownStream = false;
        synchronized (viewLock) {
            if (isReleased) {
                return;
            }
            if (needNotify) {
                notifyDownStream = true;
                needNotify = false;
            }
        }
        if (notifyDownStream) {
            availabilityListener.notifyDataAvailable();
        }
    }

    @Override
    public ResultSubpartitionView.AvailabilityWithBacklog getAvailabilityAndBacklog(
            int numCreditsAvailable) {
        synchronized (viewLock) {
            boolean availability = numCreditsAvailable > 0;
            if (numCreditsAvailable <= 0
                    && cachedNextDataType != null
                    && cachedNextDataType == Buffer.DataType.EVENT_BUFFER) {
                availability = true;
            }
            int backlog = unsynchronizedGetNumberOfQueuedBuffers();
            if (backlog == 0) {
                needNotify = true;
            }
            return new ResultSubpartitionView.AvailabilityWithBacklog(availability, backlog);
        }
    }

    @Override
    public void updateNeedNotifyStatus() {
        synchronized (viewLock) {
            if (unsynchronizedGetNumberOfQueuedBuffers() == 0) {
                needNotify = true;
            }
        }
    }

    @Override
    public void release() throws IOException {
        releaseInternal(null);
    }

    @Override
    public boolean isReleased() {
        synchronized (viewLock) {
            return isReleased;
        }
    }

    @Override
    public int getConsumingOffset(boolean withLock) {
        if (!withLock) {
            return consumingOffset;
        }
        synchronized (viewLock) {
            return consumingOffset;
        }
    }

    @Override
    public Throwable getFailureCause() {
        synchronized (viewLock) {
            return failureCause;
        }
    }

    @Override
    public int unsynchronizedGetNumberOfQueuedBuffers() {
        if (nettyBufferQueue == null) {
            return 0;
        }
        return nettyBufferQueue.getBacklog();
    }

    @Override
    public int getNumberOfQueuedBuffers() {
        synchronized (viewLock) {
            return unsynchronizedGetNumberOfQueuedBuffers();
        }
    }

    @VisibleForTesting
    public boolean getNeedNotifyStatus() {
        return needNotify;
    }

    // -------------------------------
    //       Internal Methods
    // -------------------------------

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    @GuardedBy("viewLock")
    private void updateConsumingStatus(Optional<BufferAndBacklog> bufferAndBacklog) {
        assert Thread.holdsLock(viewLock);
        // if consumed, update and check consume offset
        if (bufferAndBacklog.isPresent()) {
            ++consumingOffset;
            checkState(bufferAndBacklog.get().getSequenceNumber() == consumingOffset);
        }

        // update need notify status
        boolean dataAvailable =
                bufferAndBacklog.map(BufferAndBacklog::isDataAvailable).orElse(false);
        needNotify = !dataAvailable;
        // update cached next data type
        cachedNextDataType = bufferAndBacklog.map(BufferAndBacklog::getNextDataType).orElse(null);
    }

    private void releaseInternal(@Nullable Throwable throwable) {
        boolean releaseConsumer;
        synchronized (viewLock) {
            if (isReleased) {
                return;
            }
            isReleased = true;
            failureCause = throwable;
            releaseConsumer = nettyBufferQueue != null;
        }
        if (releaseConsumer) {
            nettyBufferQueue.release();
        }
    }
}