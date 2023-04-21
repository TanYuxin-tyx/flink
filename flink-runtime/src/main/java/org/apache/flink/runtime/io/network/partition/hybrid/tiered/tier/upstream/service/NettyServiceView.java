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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.service;

import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * For each {@link NettyBufferQueue}, there will be a corresponding {@link
 * NettyServiceView}, which will get buffer and backlog from {@link
 * NettyBufferQueue} and be aware of the available status of {@link NettyBufferQueue}.
 */
public interface NettyServiceView {

    /**
     * Get buffer and backlog from {@link NettyBufferQueue}.
     *
     * @return buffer and backlog.
     * @throws IOException is thrown if there is a failure.
     */
    @Nullable
    ResultSubpartition.BufferAndBacklog getNextBuffer() throws IOException;

    /**
     * Get availability and backlog of {@link NettyBufferQueue}.
     *
     * @param numCreditsAvailable is the available credit.
     * @return availability and backlog.
     */
    ResultSubpartitionView.AvailabilityWithBacklog getAvailabilityAndBacklog(
            int numCreditsAvailable);

    /**
     * Set the tier reader to the view.
     *
     * @param nettyBufferQueue is the tier reader.
     */
    void setNettyBufferQueue(NettyBufferQueue nettyBufferQueue);

    /** Update the need notify status of {@link NettyBufferQueue}. */
    void updateNeedNotifyStatus();

    /** Notify that {@link NettyBufferQueue} is available. */
    void notifyDataAvailable();

    /**
     * Get the current consumed buffer index.
     *
     * @param withLock indicates whether to lock to get buffer index.
     * @return current consumed buffer index.
     */
    int getConsumingOffset(boolean withLock);

    /**
     * Get the number of queued buffers in {@link NettyBufferQueue} unsynchronizedly.
     *
     * @return the number of queued buffers.
     */
    int unsynchronizedGetNumberOfQueuedBuffers();

    /**
     * Get the number of queued buffers in {@link NettyBufferQueue} synchronizedly.
     *
     * @return the number of queued buffers.
     */
    int getNumberOfQueuedBuffers();

    /**
     * Release the {@link NettyServiceView}.
     *
     * @throws IOException happened during releasing the view.
     */
    void release() throws IOException;

    /**
     * Return the release status.
     *
     * @return if the view is released.
     */
    boolean isReleased();

    /**
     * Get the failure cause when getting next buffer.
     *
     * @return the failure cause.
     */
    Throwable getFailureCause();
}
