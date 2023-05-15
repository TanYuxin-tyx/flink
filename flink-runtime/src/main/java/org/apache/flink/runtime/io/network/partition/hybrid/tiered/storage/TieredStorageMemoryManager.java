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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage;

import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.LocalBufferPool;

import java.util.List;

/**
 * The {@link TieredStorageMemoryManager} is to request or recycle buffers from {@link
 * LocalBufferPool} for different memory owners, for example, the tiers, the buffer accumulator,
 * etc. Note that the logic for requesting and recycling buffers is consistent for these owners.
 *
 * <p>The memory managed by {@link TieredStorageMemoryManager} is categorized into two types:
 * long-term occupied memory which cannot be immediately released and short-term occupied memory
 * which can be reclaimed quickly and safely. Long-term occupied memory usage necessitates waiting
 * for other operations to complete before releasing it, such as downstream consumption. On the
 * other hand, short-term occupied memory can be freed up at any time, enabling rapid memory
 * recycling for tasks such as flushing memory to disk or remote storage.
 *
 * <p>This {@link TieredStorageMemoryManager} aim to streamline and harmonize memory management
 * across various layers. Instead of tracking the number of buffers utilized by individual users, it
 * dynamically calculates a user's maximum guaranteed amount based on the current status of the
 * manager and the local buffer pool. Specifically, if a user is a long-term occupied memory user,
 * the {@link TieredStorageMemoryManager} does not limit the user's memory usage, while if a user is
 * a short-term occupied memory user, the current guaranteed buffers of the user is the left buffers
 * in the buffer pool - guaranteed amount of other users (excluding the current user).
 */
public interface TieredStorageMemoryManager {

    /**
     * Setup the {@link TieredStorageMemoryManager}. When setting up the manager, the {@link
     * TieredStorageMemorySpec}s for different tiered storages should be ready to indicate each
     * tiered storage's memory requirement specs.
     *
     * @param bufferPool the local buffer pool
     * @param storageMemorySpecs the memory specs for different tiered storages
     */
    void setup(BufferPool bufferPool, List<TieredStorageMemorySpec> storageMemorySpecs);

    /**
     * Register a buffer flush call back to flush and recycle all the memory in the user when
     * needed.
     *
     * <p>When the left buffers in the {@link BufferPool} are not enough, {@link
     * TieredStorageMemoryManager} will flush the buffers of the user with the registered callback.
     *
     * @param userBufferFlushCallback the buffer flush call back of the memory user
     */
    void registerBufferFlushCallback(Runnable userBufferFlushCallback);

    /**
     * Request a {@link BufferBuilder} instance from {@link LocalBufferPool} for a specific owner.
     *
     * @return the requested buffer
     */
    BufferBuilder requestBufferBlocking();

    /**
     * Return the available buffers for the owner.
     *
     * <p>Note that the available buffers are calculated dynamically based on some conditions, for
     * example, the state of the {@link BufferPool}, the {@link TieredStorageMemorySpec} of the
     * owner, etc.
     *
     * <p>If a user is a long-term occupied memory user, the {@link TieredStorageMemoryManager} does
     * not limit the user's memory usage, while if a user is a short-term occupied memory user, the
     * max allowed buffers of the user is the left buffers in the buffer pool - guaranteed amount of
     * other users (excluding the current user).
     */
    int getMaxAllowedBuffers(Object owner);

    /**
     * Release all the resources(if exists) and check the state of the {@link
     * TieredStorageMemoryManager}.
     */
    void release();
}
