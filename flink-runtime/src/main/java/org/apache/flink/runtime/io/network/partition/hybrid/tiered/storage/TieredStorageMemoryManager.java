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

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.LocalBufferPool;

/**
 * The {@link TieredStorageMemoryManager} is to request or recycle buffers from {@link
 * LocalBufferPool} for different memory owners, for example, the tiers, the buffer accumulator,
 * etc.
 *
 * <p>Note that the logic for requesting and recycling buffers is consistent for these owners.
 */
public interface TieredStorageMemoryManager {

    /** Setup the {@link TieredStorageMemoryManager}. */
    void setup(BufferPool bufferPool);

    /**
     * Before requesting buffers, the memory user owner should register its {@link
     * TieredStorageMemorySpec} to indicate the owner's memory specs.
     */
    void registerMemorySpec(TieredStorageMemorySpec memorySpec);

    /**
     * Request a {@link Buffer} instance from {@link LocalBufferPool} for a specific owner.
     *
     * @return the requested buffer
     */
    BufferBuilder requestBufferBlocking();

    void registerBufferFlushCallBack(Runnable userBufferFlushCallBack);

    /**
     * Return the available buffers for the owner.
     *
     * <p>Note that the available buffers are calculated dynamically based on some conditions, for
     * example, the state of the {@link BufferPool}, the {@link TieredStorageMemorySpec} of the
     * owner, etc.
     */
    int numAvailableBuffers(Object owner);

    /**
     * Release all the resources(if exists) and check the state of the {@link
     * TieredStorageMemoryManager}.
     */
    void release();
}
