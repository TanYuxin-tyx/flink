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
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.util.ExceptionUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

public class TieredStorageMemoryManagerImpl implements TieredStorageMemoryManager {

    private final Map<Object, TieredStorageMemorySpec> tieredMemorySpecs;

    private int numTotalExclusiveBuffers;

    private BufferPool bufferPool;

    private final AtomicInteger numRequestedBuffers;

    public TieredStorageMemoryManagerImpl() {
        this.tieredMemorySpecs = new HashMap<>();
        this.numRequestedBuffers = new AtomicInteger(0);
    }

    @Override
    public void setup(BufferPool bufferPool) {
        this.bufferPool = bufferPool;
    }

    @Override
    public void registerMemorySpec(TieredStorageMemorySpec memorySpec) {
        checkState(
                !tieredMemorySpecs.containsKey(memorySpec.getOwner()),
                "Duplicated memory spec registration.");
        tieredMemorySpecs.put(memorySpec.getOwner(), memorySpec);
        numTotalExclusiveBuffers += memorySpec.getNumExclusiveBuffers();
    }

    @Override
    public BufferBuilder requestBufferBlocking() {
        MemorySegment requestedBuffer = null;
        try {
            requestedBuffer = bufferPool.requestMemorySegmentBlocking();
        } catch (Throwable throwable) {
            ExceptionUtils.rethrow(throwable, "Failed to request memory segments.");
        }
        numRequestedBuffers.incrementAndGet();
        return new BufferBuilder(checkNotNull(requestedBuffer), this::recycleBuffer);
    }

    @Override
    public int numAvailableBuffers(Object owner) {
        TieredStorageMemorySpec ownerMemorySpec = checkNotNull(tieredMemorySpecs.get(owner));

        if (ownerMemorySpec.isMemoryReleasable()) {
            return Integer.MAX_VALUE;
        } else {
            int ownerExclusiveBuffers = ownerMemorySpec.getNumExclusiveBuffers();
            return bufferPool.getNumBuffers() - numTotalExclusiveBuffers + ownerExclusiveBuffers;
        }
    }

    @Override
    public int numTotalBuffers() {
        return bufferPool.getNumBuffers();
    }

    @Override
    public int numRequestedBuffers() {
        return numRequestedBuffers.get();
    }

    @Override
    public void release() {
        checkState(numRequestedBuffers.get() == 0, "Leaking buffers.");
    }

    private void recycleBuffer(MemorySegment buffer) {
        bufferPool.recycle(buffer);
        numRequestedBuffers.decrementAndGet();
    }
}
