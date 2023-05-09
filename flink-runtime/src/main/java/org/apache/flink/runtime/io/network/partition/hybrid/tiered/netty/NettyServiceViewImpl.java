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
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.BufferContext;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.Optional;
import java.util.Queue;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** The implementation of {@link NettyServiceView}. */
public class NettyServiceViewImpl implements NettyServiceView {

    private final Object viewLock = new Object();

    private final BufferAvailabilityListener availabilityListener;

    @GuardedBy("viewLock")
    private int consumingOffset = -1;

    @Nullable
    @GuardedBy("viewLock")
    private Throwable failureCause = null;

    @GuardedBy("viewLock")
    private boolean isReleased = false;

    @GuardedBy("viewLock")
    private final Queue<BufferContext> bufferQueue;

    private final Runnable releaseNotifier;

    public NettyServiceViewImpl(
            Queue<BufferContext> bufferQueue,
            BufferAvailabilityListener availabilityListener,
            Runnable releaseNotifier) {
        this.bufferQueue = bufferQueue;
        this.availabilityListener = availabilityListener;
        this.releaseNotifier = releaseNotifier;
    }

    @Nullable
    @Override
    public Optional<BufferAndBacklog> getNextBuffer() throws IOException {
        try {
            synchronized (viewLock) {
                Optional<BufferAndBacklog> bufferToConsume;
                if (!checkBufferIndex(consumingOffset + 1).isPresent()) {
                    bufferToConsume = Optional.empty();
                } else {
                    BufferContext current = checkNotNull(bufferQueue.poll());
                    Buffer.DataType nextDataType =
                            bufferQueue.isEmpty()
                                    ? Buffer.DataType.NONE
                                    : checkNotNull(bufferQueue.peek().getBuffer()).getDataType();
                    bufferToConsume =
                            Optional.of(
                                    BufferAndBacklog.fromBufferAndLookahead(
                                            current.getBuffer(),
                                            nextDataType,
                                            bufferQueue.size(),
                                            current.getBufferIndex()));
                    ++consumingOffset;
                    checkState(bufferToConsume.get().getSequenceNumber() == consumingOffset);
                }
                return bufferToConsume;
            }
        } catch (Throwable cause) {
            releaseInternal(cause);
            throw new IOException("Failed to get next buffer.", cause);
        }
    }

    @Override
    public void notifyDataAvailable() {
        synchronized (viewLock) {
            if (isReleased) {
                return;
            }
        }
        availabilityListener.notifyDataAvailable();
    }

    @Override
    public ResultSubpartitionView.AvailabilityWithBacklog getAvailabilityAndBacklog(
            int numCreditsAvailable) {
        synchronized (viewLock) {
            boolean availability = numCreditsAvailable > 0;
            Buffer.DataType nextDataType;
            if (bufferQueue.isEmpty()) {
                nextDataType = Buffer.DataType.NONE;
            } else {
                nextDataType = checkNotNull(bufferQueue.peek().getBuffer()).getDataType();
            }
            if (numCreditsAvailable <= 0 && nextDataType == Buffer.DataType.EVENT_BUFFER) {
                availability = true;
            }
            int backlog = getNumberOfQueuedBuffers();
            return new ResultSubpartitionView.AvailabilityWithBacklog(availability, backlog);
        }
    }

    @Override
    public void release() throws IOException {
        releaseInternal(null);
    }

    @Override
    public Throwable getFailureCause() {
        synchronized (viewLock) {
            return failureCause;
        }
    }

    @Override
    public int getNumberOfQueuedBuffers() {
        synchronized (viewLock) {
            return bufferQueue.size();
        }
    }

    // -------------------------------
    //       Internal Methods
    // -------------------------------

    private void releaseInternal(@Nullable Throwable throwable) {
        synchronized (viewLock) {
            if (isReleased) {
                return;
            }
            isReleased = true;
            BufferContext bufferContext;
            while ((bufferContext = bufferQueue.poll()) != null) {
                if (bufferContext.getBuffer() != null) {
                    checkNotNull(bufferContext.getBuffer()).recycleBuffer();
                }
            }
            failureCause = throwable;
        }
        releaseNotifier.run();
    }

    private Optional<BufferContext> checkBufferIndex(int expectedBufferIndex) throws Throwable {
        if (bufferQueue.isEmpty()) {
            return Optional.empty();
        }
        BufferContext peek = bufferQueue.peek();
        if (peek.getError() != null) {
            throw peek.getError();
        }
        checkState(peek.getBufferIndex() == expectedBufferIndex);
        return Optional.of(peek);
    }
}
