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

import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView.AvailabilityWithBacklog;

import java.io.IOException;
import java.util.Optional;

/**
 * {@link NettyServiceView} is the coordinator between a tier agent and the netty server. Netty
 * server could get the buffer and get available status of data from it, and tier agent could notify
 * it when the data is available.
 */
public interface NettyServiceView {

    /**
     * Get buffer and backlog.
     *
     * @return buffer and backlog.
     * @throws IOException is thrown if there is a failure.
     */
    Optional<BufferAndBacklog> getNextBuffer() throws IOException;

    /**
     * Get availability and backlog.
     *
     * @param numCreditsAvailable is the available credit.
     * @return availability and backlog.
     */
    AvailabilityWithBacklog getAvailabilityAndBacklog(int numCreditsAvailable);

    /** Notify that the view is available. */
    void notifyDataAvailable();

    /**
     * Get the number of queued buffers.
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
}
