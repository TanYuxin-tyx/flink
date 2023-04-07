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
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.TierType;

/** Test implementation for {@link TieredStoreMemoryManager}. */
public class TestingTieredStoreMemoryManager implements TieredStoreMemoryManager {
    @Override
    public void setBufferPool(BufferPool bufferPool) {}

    @Override
    public int numAvailableBuffers(TierType tierType) {
        return 0;
    }

    @Override
    public int numRequestedBuffers() {
        return 0;
    }

    @Override
    public int numTotalBuffers() {
        return 0;
    }

    @Override
    public float numBuffersTriggerFlushRatio() {
        return 0;
    }

    @Override
    public MemorySegment requestMemorySegmentBlocking(TierType tierType) {
        return null;
    }

    @Override
    public MemorySegment requestMemorySegmentInAccumulatorBlocking() {
        return null;
    }

    @Override
    public void recycleBufferInAccumulator(MemorySegment memorySegment) {}

    @Override
    public void incNumRequestedBufferInAccumulator() {}

    @Override
    public void decRequestedBufferInAccumulator() {}

    @Override
    public void recycleBuffer(MemorySegment memorySegment, TierType tierType) {}

    @Override
    public void incNumRequestedBuffer(TierType tierType) {}

    @Override
    public void decNumRequestedBuffer(TierType tierType) {}

    @Override
    public void checkNeedTriggerFlushCachedBuffers() {}

    @Override
    public void close() {}

    @Override
    public void release() {}
}
