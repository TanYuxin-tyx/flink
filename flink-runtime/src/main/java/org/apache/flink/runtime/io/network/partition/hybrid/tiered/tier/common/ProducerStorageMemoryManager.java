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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.common;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.util.ExceptionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

public class ProducerStorageMemoryManager implements StorageMemoryManager {

    private final Map<Integer, TierMemorySpec> tierMemorySpecMap;

    private final Map<Integer, Integer> tierExclusiveBuffers;

    private final Map<Integer, AtomicInteger> tierRequestedBuffersCounter;

    private final AtomicInteger numRequestedBuffersInAccumulator;

    private final AtomicInteger numRequestedBuffers;

    private final int numSubpartitions;

    private int numTotalExclusiveBuffers;

    private BufferPool bufferPool;

    public ProducerStorageMemoryManager(
            int numSubpartitions, List<TierMemorySpec> tierMemorySpecs) {
        this.numSubpartitions = numSubpartitions;
        this.tierMemorySpecMap = new HashMap<>();
        this.tierExclusiveBuffers = new HashMap<>();
        this.tierRequestedBuffersCounter = new HashMap<>();
        this.numRequestedBuffersInAccumulator = new AtomicInteger(0);
        this.numRequestedBuffers = new AtomicInteger(0);

        for (TierMemorySpec tierMemorySpec : tierMemorySpecs) {
            checkState(
                    !tierMemorySpecMap.containsKey(tierMemorySpec.getTierIndex()),
                    "Duplicate tier indexes.");
            tierMemorySpecMap.put(tierMemorySpec.getTierIndex(), tierMemorySpec);
            tierExclusiveBuffers.put(
                    tierMemorySpec.getTierIndex(), tierMemorySpec.getNumExclusiveBuffers());
            tierRequestedBuffersCounter.put(tierMemorySpec.getTierIndex(), new AtomicInteger(0));
            numTotalExclusiveBuffers += tierMemorySpec.getNumExclusiveBuffers();
        }
    }

    @Override
    public void setup(BufferPool bufferPool) {
        this.bufferPool = bufferPool;
    }

    @Override
    public MemorySegment requestBufferBlocking(int tierIndex) {
        MemorySegment requestedBuffer = null;
        try {
            requestedBuffer = bufferPool.requestMemorySegmentBlocking();
        } catch (Throwable throwable) {
            ExceptionUtils.rethrow(throwable, "Failed to request memory segments.");
        }
        incNumRequestedBuffer(tierIndex);
        return requestedBuffer;
    }

    @Override
    public void recycleBuffer(MemorySegment memorySegment, int tierIndex) {
        bufferPool.recycle(memorySegment);
        decNumRequestedBuffer(tierIndex);
    }

    @Override
    public MemorySegment requestBufferInAccumulator() {
        MemorySegment requestedBuffer = null;
        try {
            requestedBuffer = bufferPool.requestMemorySegmentBlocking();
        } catch (Throwable throwable) {
            ExceptionUtils.rethrow(throwable, "Failed to request memory segments.");
        }
        incNumRequestedBufferInAccumulator();
        return requestedBuffer;
    }

    @Override
    public void recycleBufferInAccumulator(MemorySegment memorySegment) {
        bufferPool.recycle(memorySegment);
        decNumRequestedBufferInAccumulator();
    }

    @Override
    public void incNumRequestedBuffer(int tierIndex) {
        numRequestedBuffers.getAndIncrement();
        checkNotNull(tierRequestedBuffersCounter.get(tierIndex)).incrementAndGet();
    }

    @Override
    public void decNumRequestedBuffer(int tierIndex) {
        numRequestedBuffers.decrementAndGet();
        checkNotNull(tierRequestedBuffersCounter.get(tierIndex)).decrementAndGet();
    }

    @Override
    public void incNumRequestedBufferInAccumulator() {
        numRequestedBuffers.getAndIncrement();
        numRequestedBuffersInAccumulator.getAndIncrement();
    }

    @Override
    public void decNumRequestedBufferInAccumulator() {
        numRequestedBuffers.decrementAndGet();
        numRequestedBuffersInAccumulator.decrementAndGet();
    }

    @Override
    public int numAvailableBuffers(int tierIndex) {
        TierMemorySpec tierMemorySpec = checkNotNull(tierMemorySpecMap.get(tierIndex));
        int numExclusive = tierMemorySpec.getNumExclusiveBuffers();
        boolean canUseSharedBuffers = tierMemorySpec.canUseShareBuffers();
        int numRequested = checkNotNull(tierRequestedBuffersCounter.get(tierIndex)).get();

        if (!canUseSharedBuffers) {
            return numExclusive >= numRequested ? numExclusive - numRequested : 0;
        } else {
            return numExclusive + bufferPool.getNumBuffers() >= numRequested
                    ? numExclusive + bufferPool.getNumBuffers() - numRequested
                    : 0;
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
    public void close() {
        tierRequestedBuffersCounter.forEach(
                (k, v) -> checkState(v.get() == 0, "Leaking buffers in tier " + k));
        checkState(
                numRequestedBuffersInAccumulator.get() == 0,
                "Leaking buffers in buffer accumulator.");
        checkState(numRequestedBuffers.get() == 0, "Leaking buffers.");
    }
}
