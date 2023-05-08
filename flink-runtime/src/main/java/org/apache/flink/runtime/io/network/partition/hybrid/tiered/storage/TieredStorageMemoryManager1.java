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
 * The {@link TieredStorageMemoryManager1} is to request or recycle buffer from {@link
 * LocalBufferPool} for different memory users, such as the tiers or the buffer accumulator. Note
 * that the logic for requesting and recycling buffers remains consistent for these users.
 */
public interface TieredStorageMemoryManager1 {

    /** Setup the {@link TieredStorageMemoryManager1}. */
    void setup(BufferPool bufferPool);

    void registerMemorySpec(TieredStorageMemorySpec memorySpec);

    /**
     * Request a {@link Buffer} instance from {@link LocalBufferPool} for a specific owner.
     *
     * @param owner the owner to request new buffer
     * @return the requested buffer
     */
    BufferBuilder requestBufferBlocking(Object owner);

    int numAvailableBuffers(Object owner);

    int numTotalBuffers();

    int numRequestedBuffers();

    void release();
}
