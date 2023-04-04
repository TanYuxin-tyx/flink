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
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FatalExitExceptionHandler;

import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreUtils.needFlushCacheBuffers;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** Upstream tasks will get buffer from this {@link UpstreamTieredStoreMemoryManager}. */
public class UpstreamTieredStoreMemoryManager implements TieredStoreMemoryManager {

    private final BufferPool bufferPool;

    private final Map<TieredStoreMode.TierType, Integer> tierExclusiveBuffers;

    private final Map<TieredStoreMode.TierType, AtomicInteger> tierRequestedBuffersCounter;

    private final CacheFlushManager cacheFlushManager;

    private final AtomicInteger numRequestedBuffers = new AtomicInteger(0);

    private final int numSubpartitions;

    private final int numTotalExclusiveBuffers;

    private final ScheduledExecutorService executor =
            Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder()
                            .setNameFormat("upstream tiered store memory manager notifier")
                            .setUncaughtExceptionHandler(FatalExitExceptionHandler.INSTANCE)
                            .build());

    public UpstreamTieredStoreMemoryManager(
            BufferPool bufferPool,
            Map<TieredStoreMode.TierType, Integer> tierExclusiveBuffers,
            int numSubpartitions,
            CacheFlushManager cacheFlushManager) {
        this.bufferPool = bufferPool;
        this.tierExclusiveBuffers = tierExclusiveBuffers;
        this.tierRequestedBuffersCounter = new HashMap<>();
        this.cacheFlushManager = cacheFlushManager;
        this.numSubpartitions = numSubpartitions;
        this.numTotalExclusiveBuffers =
                tierExclusiveBuffers.values().stream().mapToInt(i -> i).sum();
        executor.scheduleWithFixedDelay(
                this::checkNeedTriggerFlushCachedBuffers, 10, 50, TimeUnit.MILLISECONDS);
    }

    public BufferPool getBufferPool() {
        return bufferPool;
    }

    @Override
    public int numAvailableBuffers(TieredStoreMode.TierType tierType) {
        int numTotalBuffers = bufferPool.getNumBuffers();
        switch (tierType) {
            case IN_CACHE:
                return getAvailableBuffersForCache(numTotalBuffers);
            case IN_MEM:
                return getAvailableBuffersForMemory(numTotalBuffers);
            case IN_DISK:
                return getAvailableBuffersForDisk(numTotalBuffers);
            case IN_REMOTE:
                return getAvailableBuffersForRemote(numTotalBuffers);
            default:
                throw new RuntimeException("Unsupported tiered type " + tierType);
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
    public MemorySegment requestMemorySegmentBlocking(TieredStoreMode.TierType tierType) {
        MemorySegment requestedBuffer;
        try {
            requestedBuffer = bufferPool.requestMemorySegmentBlocking();
        } catch (Throwable throwable) {
            throw new RuntimeException("Failed to request memory segments.", throwable);
        }
        incNumRequestedBuffer(tierType);
        checkNeedTriggerFlushCachedBuffers();
        return requestedBuffer;
    }

    @Override
    public void incNumRequestedBuffer(TieredStoreMode.TierType tierType) {
        numRequestedBuffers.getAndIncrement();
        tierRequestedBuffersCounter.putIfAbsent(tierType, new AtomicInteger(0));
        tierRequestedBuffersCounter.get(tierType).incrementAndGet();
    }

    @Override
    public void decNumRequestedBuffer(TieredStoreMode.TierType tierType) {
        numRequestedBuffers.decrementAndGet();
        AtomicInteger numRequestedBuffers = tierRequestedBuffersCounter.get(tierType);
        checkNotNull(numRequestedBuffers).decrementAndGet();
    }

    @Override
    public void recycleBuffer(MemorySegment memorySegment, TieredStoreMode.TierType tierType) {
        bufferPool.recycle(memorySegment);
        decNumRequestedBuffer(tierType);
    }

    @Override
    public void checkNeedTriggerFlushCachedBuffers() {
        if (needFlushCacheBuffers(this)) {
            cacheFlushManager.triggerFlushCachedBuffers();
        }
    }

    @Override
    public void close() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5L, TimeUnit.MINUTES)) {
                throw new TimeoutException("Shutdown spilling thread timeout.");
            }
        } catch (Exception e) {
            ExceptionUtils.rethrow(e);
        }
    }

    @Override
    public void release() {
        checkState(numRequestedBuffers.get() == 0, "Leaking buffers.");
        for (Map.Entry<TieredStoreMode.TierType, AtomicInteger> tierRequestedBuffer :
                tierRequestedBuffersCounter.entrySet()) {
            checkState(
                    tierRequestedBuffer.getValue().get() == 0,
                    "Leaking buffers in tier " + tierRequestedBuffer.getKey());
        }
    }

    private int getAvailableBuffersForCache(int numAvailableBuffers) {
        return numAvailableBuffers - numTotalExclusiveBuffers;
    }

    // Available - numSubpartitions + numExclusiveBuffersInMem - numRequestedFromMem
    private int getAvailableBuffersForMemory(int numAvailableBuffers) {
        AtomicInteger numRequestedBuffersInteger =
                tierRequestedBuffersCounter.get(TieredStoreMode.TierType.IN_MEM);
        int numRequestedBuffers =
                numRequestedBuffersInteger == null ? 0 : numRequestedBuffersInteger.get();
        return Math.max(
                numAvailableBuffers
                        - numSubpartitions
                        + checkNotNull(tierExclusiveBuffers.get(TieredStoreMode.TierType.IN_MEM))
                        - numRequestedBuffers,
                0);
    }

    // numExclusiveBuffersInDisk - numRequestedFromDisk + (Available - (numExclusiveBuffersInMem -
    // numRequestedFromMem))
    private int getAvailableBuffersForDisk(int numAvailableBuffers) {
        return getAvailableBuffers(numAvailableBuffers, TieredStoreMode.TierType.IN_DISK);
    }

    // numExclusiveBuffersInRemote - numRequestedFromRemote + (Available - (numExclusiveBuffersInMem
    // - numRequestedFromMem))
    private int getAvailableBuffersForRemote(int numAvailableBuffers) {
        return getAvailableBuffers(numAvailableBuffers, TieredStoreMode.TierType.IN_REMOTE);
    }

    private int getAvailableBuffers(int numAvailableBuffers, TieredStoreMode.TierType tierType) {
        int numExclusive = checkNotNull(tierExclusiveBuffers.get(tierType));
        int numExclusiveForMemory =
                checkNotNull(tierExclusiveBuffers.get(TieredStoreMode.TierType.IN_MEM));
        AtomicInteger numRequestedFromMemInteger =
                tierRequestedBuffersCounter.get(TieredStoreMode.TierType.IN_MEM);
        int numRequestedFromMemory =
                numRequestedFromMemInteger == null ? 0 : numRequestedFromMemInteger.get();
        AtomicInteger numRequestedInteger = tierRequestedBuffersCounter.get(tierType);
        int numRequested = numRequestedInteger == null ? 0 : numRequestedInteger.get();

        int numLeftExclusiveForMemory = Math.max(numExclusiveForMemory - numRequestedFromMemory, 0);
        return numExclusive - numRequested + (numAvailableBuffers - numLeftExclusiveForMemory);
    }
}
