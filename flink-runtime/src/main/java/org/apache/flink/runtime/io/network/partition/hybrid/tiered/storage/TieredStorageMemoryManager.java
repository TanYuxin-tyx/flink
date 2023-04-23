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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.LocalBufferPool;

public interface TieredStorageMemoryManager {

    void setup(BufferPool bufferPool);

    /** Requests a {@link MemorySegment} instance from {@link LocalBufferPool}. */
    MemorySegment requestBufferBlocking(int tierIndex);

    void recycleBuffer(MemorySegment memorySegment, int tierIndex);

    /** Requests a {@link MemorySegment} instance from {@link LocalBufferPool}. */
    MemorySegment requestBufferInAccumulator();

    void recycleBufferInAccumulator(MemorySegment memorySegment);

    void incNumRequestedBuffer(int tierIndex);

    void decNumRequestedBuffer(int tierIndex);

    void incNumRequestedBufferInAccumulator();

    void decNumRequestedBufferInAccumulator();

    /** Returns the available buffers for specific tier. */
    int numAvailableBuffers(int tierIndex);

    /** Returns the total buffers of this {@link LocalBufferPool}. */
    int numTotalBuffers();

    /** Returns the total requested buffers. */
    int numRequestedBuffers();

    void release();
}
