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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.LocalBufferPool;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.TierType;

/**
 * The helper allocating and recycling buffer from {@link LocalBufferPool} to different tiers,
 * including local memory, disk, remote storage, etc.
 */
public interface TieredStoreMemoryManager {

    void setBufferPool(BufferPool bufferPool);

    /** Returns the available buffers for this {@link TierType}. */
    int numAvailableBuffers(TierType tierType);

    /** Returns the total requested buffers. */
    int numRequestedBuffers();

    /** Returns the total buffers of this {@link LocalBufferPool}. */
    int numTotalBuffers();

    /** Returns the ratio to trigger flush the cached buffers. */
    float numBuffersTriggerFlushRatio();

    /** Requests a {@link MemorySegment} instance from {@link LocalBufferPool}. */
    MemorySegment requestMemorySegmentBlocking(TierType tierType);

    void recycleBuffer(MemorySegment memorySegment, TierType tierType);

    void incNumRequestedBuffer(TierType tierType);

    void decNumRequestedBuffer(TierType tierType);

    /** Checks whether the cached buffers should be flushed. */
    void checkNeedTriggerFlushCachedBuffers();

    void close();

    void release();
}
