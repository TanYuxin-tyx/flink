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
import org.apache.flink.runtime.io.network.buffer.LocalBufferPool;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FatalExitExceptionHandler;

import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The implementation for {@link TieredStorageMemoryManager}. This is to request or recycle buffers
 * from {@link LocalBufferPool} for different memory owners, for example, the tiers, the buffer
 * accumulator, etc.
 *
 * <p>Note that the memory owner should register its {@link TieredStorageMemorySpec} firstly before
 * requesting buffers.
 */
public class TieredStorageMemoryManagerImpl implements TieredStorageMemoryManager {

    private final Map<Object, TieredStorageMemorySpec> tieredMemorySpecs;

    private final List<CacheBufferFlushTrigger> flushTriggers;

    private final float numBuffersTriggerFlushRatio;

    private final AtomicInteger numRequestedBuffers;

    private final ScheduledExecutorService executor;

    private int numTotalExclusiveBuffers;

    private BufferPool bufferPool;

    public TieredStorageMemoryManagerImpl(float numBuffersTriggerFlushRatio) {
        this.tieredMemorySpecs = new HashMap<>();
        this.numRequestedBuffers = new AtomicInteger(0);
        this.flushTriggers = new ArrayList<>();
        this.numBuffersTriggerFlushRatio = numBuffersTriggerFlushRatio;

        this.executor =
                Executors.newSingleThreadScheduledExecutor(
                        new ThreadFactoryBuilder()
                                .setNameFormat("cache flush trigger")
                                .setUncaughtExceptionHandler(FatalExitExceptionHandler.INSTANCE)
                                .build());
        this.executor.scheduleWithFixedDelay(
                this::checkNeedTriggerFlushCachedBuffers, 10, 50, TimeUnit.MILLISECONDS);
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
        checkNeedTriggerFlushCachedBuffers();
        return new BufferBuilder(checkNotNull(requestedBuffer), this::recycleBuffer);
    }

    @Override
    public void registerCacheBufferFlushTrigger(CacheBufferFlushTrigger cacheBufferFlushTrigger) {
        flushTriggers.add(cacheBufferFlushTrigger);
    }

    private void checkNeedTriggerFlushCachedBuffers() {
        if (needFlushCacheBuffers()) {
            flushTriggers.forEach(CacheBufferFlushTrigger::notifyFlushCachedBuffers);
        }
    }

    @Override
    public int numAvailableBuffers(Object owner) {
        TieredStorageMemorySpec ownerMemorySpec = checkNotNull(tieredMemorySpecs.get(owner));

        if (ownerMemorySpec.isMemoryReleasable()) {
            return Integer.MAX_VALUE;
        } else {
            int ownerExclusiveBuffers = ownerMemorySpec.getNumExclusiveBuffers();
            return bufferPool.getNumBuffers()
                    - numRequestedBuffers.get()
                    - numTotalExclusiveBuffers
                    + ownerExclusiveBuffers;
        }
    }

    @Override
    public void release() {
        checkState(numRequestedBuffers.get() == 0, "Leaking buffers.");
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5L, TimeUnit.MINUTES)) {
                throw new TimeoutException("Timeout for shutting down the cache flush executor.");
            }
        } catch (Exception e) {
            ExceptionUtils.rethrow(e);
        }
    }

    private boolean needFlushCacheBuffers() {
        synchronized (this) {
            int numTotal = bufferPool.getNumBuffers();
            int numRequested = numRequestedBuffers.get();
            return numRequested >= numTotal
                    || (numRequested * 1.0 / numTotal) >= numBuffersTriggerFlushRatio;
        }
    }

    private void recycleBuffer(MemorySegment buffer) {
        bufferPool.recycle(buffer);
        numRequestedBuffers.decrementAndGet();
    }
}
