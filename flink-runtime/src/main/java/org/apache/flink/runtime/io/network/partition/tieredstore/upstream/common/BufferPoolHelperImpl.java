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

package org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.TieredStoreMode;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** All buffers of Tiered Store are acquired from this {@link BufferPoolHelperImpl}. */
public class BufferPoolHelperImpl implements BufferPoolHelper {

    private final BufferPool bufferPool;

    private final Map<TieredStoreMode.TieredType, Integer> tierExclusiveBuffers;

    private final Map<TieredStoreMode.TieredType, AtomicInteger> tierRequestedBuffersCounter;

    private final int numSubpartitions;

    public BufferPoolHelperImpl(
            BufferPool bufferPool,
            Map<TieredStoreMode.TieredType, Integer> tierExclusiveBuffers,
            int numSubpartitions) {
        this.bufferPool = bufferPool;
        this.tierExclusiveBuffers = tierExclusiveBuffers;
        this.tierRequestedBuffersCounter = new HashMap<>();
        this.numSubpartitions = numSubpartitions;
    }

    @Override
    public int numAvailableBuffers(TieredStoreMode.TieredType tieredType) {
        int numAvailableBuffers = bufferPool.getNumberOfAvailableMemorySegments();
        switch (tieredType) {
            case IN_MEM:
                return getAvailableBuffersForMemory(numAvailableBuffers);
            case IN_LOCAL:
                return getAvailableBuffersForDisk(numAvailableBuffers);
            case IN_DFS:
                return getAvailableBuffersForRemote(numAvailableBuffers);
            default:
                throw new RuntimeException("Unsupported tiered type " + tieredType);
        }
    }

    @Override
    public int numAvailableBuffers() {
        return bufferPool.getNumberOfAvailableMemorySegments();
    }

    @Override
    public int numTotalBuffers() {
        return bufferPool.getNumBuffers();
    }

    @Override
    public MemorySegment requestMemorySegmentBlocking(TieredStoreMode.TieredType tieredType) {
        MemorySegment requestedBuffer;
        try {
            requestedBuffer = bufferPool.requestMemorySegmentBlocking();
        } catch (Throwable throwable) {
            throw new RuntimeException("Failed to request memory segments.", throwable);
        }
        incRequestedBufferCounter(tieredType);
        return requestedBuffer;
    }

    @Override
    public void recycleBuffer(MemorySegment buffer, TieredStoreMode.TieredType tieredType) {
        bufferPool.recycle(buffer);
        decRequestedBufferCounter(tieredType);
    }

    @Override
    public void close() {}

    private void incRequestedBufferCounter(TieredStoreMode.TieredType tieredType) {
        tierRequestedBuffersCounter.putIfAbsent(tieredType, new AtomicInteger(0));
        tierRequestedBuffersCounter.get(tieredType).incrementAndGet();
    }

    private void decRequestedBufferCounter(TieredStoreMode.TieredType tieredType) {
        AtomicInteger numRequestedBuffers = tierRequestedBuffersCounter.get(tieredType);
        checkNotNull(numRequestedBuffers).decrementAndGet();
    }

    // Available - numSubpartitions + numExclusiveBuffersInMem - numRequestedFromMem
    private int getAvailableBuffersForMemory(int numAvailableBuffers) {
        AtomicInteger numRequestedBuffersInteger =
                tierRequestedBuffersCounter.get(TieredStoreMode.TieredType.IN_MEM);
        int numRequestedBuffers =
                numRequestedBuffersInteger == null ? 0 : numRequestedBuffersInteger.get();
        return Math.max(
                numAvailableBuffers
                        - numSubpartitions
                        + checkNotNull(tierExclusiveBuffers.get(TieredStoreMode.TieredType.IN_MEM))
                        - numRequestedBuffers,
                0);
    }

    // numExclusiveBuffersInDisk - numRequestedFromDisk + (Available - (numExclusiveBuffersInMem -
    // numRequestedFromMem))
    private int getAvailableBuffersForDisk(int numAvailableBuffers) {
        return getAvailableBuffers(numAvailableBuffers, TieredStoreMode.TieredType.IN_LOCAL);
    }

    // numExclusiveBuffersInRemote - numRequestedFromRemote + (Available - (numExclusiveBuffersInMem
    // - numRequestedFromMem))
    private int getAvailableBuffersForRemote(int numAvailableBuffers) {
        return getAvailableBuffers(numAvailableBuffers, TieredStoreMode.TieredType.IN_DFS);
    }

    private int getAvailableBuffers(
            int numAvailableBuffers, TieredStoreMode.TieredType tieredType) {
        int numExclusive = checkNotNull(tierExclusiveBuffers.get(tieredType));
        int numExclusiveForMemory =
                checkNotNull(tierExclusiveBuffers.get(TieredStoreMode.TieredType.IN_MEM));
        AtomicInteger numRequestedFromMemInteger =
                tierRequestedBuffersCounter.get(TieredStoreMode.TieredType.IN_MEM);
        int numRequestedFromMemory =
                numRequestedFromMemInteger == null ? 0 : numRequestedFromMemInteger.get();
        AtomicInteger numRequestedInteger = tierRequestedBuffersCounter.get(tieredType);
        int numRequested = numRequestedInteger == null ? 0 : numRequestedInteger.get();

        int numLeftExclusiveForMemory = Math.max(numExclusiveForMemory - numRequestedFromMemory, 0);
        return numExclusive - numRequested + (numAvailableBuffers - numLeftExclusiveForMemory);
    }
}
