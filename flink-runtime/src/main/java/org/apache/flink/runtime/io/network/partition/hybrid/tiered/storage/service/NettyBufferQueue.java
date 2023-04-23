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

import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;

import java.util.Optional;

/**
 * The {@link NettyBufferQueue} is invoked by {@link NettyServiceView} to get buffer
 * and backlog from a tier.
 */
public interface NettyBufferQueue {

    /**
     * Get buffer from the tier.
     *
     * @param bufferIndex the buffer index to consume.
     * @return the required buffer.
     * @throws Throwable happened during getting next buffer.
     */
    Optional<BufferAndBacklog> getNextBuffer(int bufferIndex) throws Throwable;

    /**
     * Get backlog in the tier.
     *
     * @return backlog number.
     */
    int getBacklog();

    /** Release the consumer. */
    void release();
}
