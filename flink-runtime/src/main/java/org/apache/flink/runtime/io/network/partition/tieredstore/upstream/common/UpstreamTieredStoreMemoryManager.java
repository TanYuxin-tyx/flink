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

    private final Map<TieredStoreMode.TieredType, Integer> tierExclusiveBuffers;

    private final Map<TieredStoreMode.TieredType, AtomicInteger> tierRequestedBuffersCounter;

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
            Map<TieredStoreMode.TieredType, Integer> tierExclusiveBuffers,
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
                UpstreamTieredStoreMemoryManager.this::checkNeedTriggerFlushCachedBuffers,
                10,
                50,
                TimeUnit.MILLISECONDS);
    }

    public BufferPool getBufferPool() {
        return bufferPool;
    }

    @Override
    public int numAvailableBuffers(TieredStoreMode.TieredType tieredType) {
        int numTotalBuffers = bufferPool.getNumBuffers();
        switch (tieredType) {
            case IN_CACHE:
                return getAvailableBuffersForCache(numTotalBuffers);
            case IN_MEM:
                return getAvailableBuffersForMemory(numTotalBuffers);
            case IN_LOCAL:
                return getAvailableBuffersForDisk(numTotalBuffers);
            case IN_DFS:
                return getAvailableBuffersForRemote(numTotalBuffers);
            default:
                throw new RuntimeException("Unsupported tiered type " + tieredType);
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
    public MemorySegment requestMemorySegmentBlocking(TieredStoreMode.TieredType tieredType) {
        MemorySegment requestedBuffer;
        try {
            requestedBuffer = bufferPool.requestMemorySegmentBlocking();
        } catch (Throwable throwable) {
            throw new RuntimeException("Failed to request memory segments.", throwable);
        }
        incRequestedBufferCounter(tieredType);
        checkNeedTriggerFlushCachedBuffers();
        return requestedBuffer;
    }

    @Override
    public int getNetworkBufferPoolAvailableBuffers() {
        return bufferPool.getNetworkBufferPoolAvailableBuffers();
    }

    @Override
    public int getNetworkBufferPoolTotalBuffers() {
        return bufferPool.getNetworkBufferPoolTotalBuffers();
    }

    @Override
    public void recycleBuffer(MemorySegment memorySegment, TieredStoreMode.TieredType tieredType) {
        bufferPool.recycle(memorySegment);
        decRequestedBufferCounter(tieredType);
    }

    @Override
    public void close() {}

    @Override
    public void release() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5L, TimeUnit.MINUTES)) {
                throw new TimeoutException("Shutdown spilling thread timeout.");
            }
        } catch (Exception e) {
            ExceptionUtils.rethrow(e);
        }
        checkState(numRequestedBuffers.get() == 0, "Leaking buffers.");
        for (Map.Entry<TieredStoreMode.TieredType, AtomicInteger> tierRequestedBuffer :
                tierRequestedBuffersCounter.entrySet()) {
            checkState(
                    tierRequestedBuffer.getValue().get() == 0,
                    "Leaking buffers in tier " + tierRequestedBuffer.getKey());
        }
    }

    private void checkNeedTriggerFlushCachedBuffers() {
        if (needFlushCacheBuffers(this)) {
            cacheFlushManager.triggerFlushCachedBuffers();
        }
    }

    private void incRequestedBufferCounter(TieredStoreMode.TieredType tieredType) {
        numRequestedBuffers.getAndIncrement();
        tierRequestedBuffersCounter.putIfAbsent(tieredType, new AtomicInteger(0));
        tierRequestedBuffersCounter.get(tieredType).incrementAndGet();
    }

    private void decRequestedBufferCounter(TieredStoreMode.TieredType tieredType) {
        numRequestedBuffers.decrementAndGet();
        AtomicInteger numRequestedBuffers = tierRequestedBuffersCounter.get(tieredType);
        checkNotNull(numRequestedBuffers).decrementAndGet();
    }

    private int getAvailableBuffersForCache(int numAvailableBuffers) {
        return numAvailableBuffers - numTotalExclusiveBuffers;
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
