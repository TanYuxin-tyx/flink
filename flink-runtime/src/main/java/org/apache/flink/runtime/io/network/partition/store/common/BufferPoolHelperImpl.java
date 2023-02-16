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

package org.apache.flink.runtime.io.network.partition.store.common;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.store.TieredStoreMode;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * All buffers of Tiered Store are acquired from this {@link BufferPoolHelperImpl}. The buffers
 * mainly include two types, the first is the memory buffers and the second is the cached buffers.
 * If the memory buffer usage or the cached buffer usage exceed the ratio, no more buffers can be
 * acquired. If the total buffer usage exceeds the flush ratio, cached buffers will be flushed to
 * the corresponding disk files or DFS files to release the cached buffers.
 */
public class BufferPoolHelperImpl implements BufferPoolHelper {

    private static final Logger LOG = LoggerFactory.getLogger(BufferPoolHelperImpl.class);

    // ------------------------------------
    //          For Local Memory Tier
    // ------------------------------------

    private final float bufferInMemoryRatio;

    private int numInMemoryMaxBuffers;

    private final int[] memoryTierSubpartitionRequiredBuffers;

    private final AtomicInteger numInMemoryBuffers = new AtomicInteger(0);

    private int bufferNumberInSegment = -1;

    // ------------------------------------
    //          For Local Disk Tier
    // ------------------------------------

    private static final int ALL_SUBPARTITIONS_INDEX = -1;

    private final BufferPool bufferPool;

    private final float flushBufferRatio;

    private final float triggerFlushRatio;

    private int numTotalBuffers;

    private int numStopNotifyFlushBuffers;

    private int numTriggerFlushBuffers;

    private final AtomicInteger numTotalCacheBuffers = new AtomicInteger(0);

    // ------------------------------------
    //                Common
    // ------------------------------------

    private final Map<SubpartitionTier, SubpartitionBuffersCounter> subpartitionCachedBuffersMap =
            new HashMap<>();

    private final Queue<CompletableFuture<Void>> requestingBuffersQueue = new LinkedList<>();

    private Queue<SubpartitionBuffersCounter> subpartitionBuffersCounters =
            new PriorityBlockingQueue<>();

    private CompletableFuture<Void> isTriggeringFlush = FutureUtils.completedVoidFuture();

    private static final int poolSizeCheckInterval = 500;

    private final ScheduledExecutorService poolSizeChecker =
            Executors.newSingleThreadScheduledExecutor(
                    new ExecutorThreadFactory("tiered-store-buffer-pool-checker"));

    public BufferPoolHelperImpl(
            BufferPool bufferPool,
            float bufferInMemoryRatio,
            float flushBufferRatio,
            float triggerFlushRatio,
            int numSubpartitions) {

        this.bufferPool = bufferPool;
        this.bufferInMemoryRatio = bufferInMemoryRatio;
        this.flushBufferRatio = flushBufferRatio;
        this.triggerFlushRatio = triggerFlushRatio;
        this.numTotalBuffers = this.bufferPool.getNumBuffers();
        this.memoryTierSubpartitionRequiredBuffers = new int[numSubpartitions];
        Arrays.fill(memoryTierSubpartitionRequiredBuffers, 0);
        // this.memoryTierSubpartitionRequiredBuffers = new int[numSubpartitions];

        checkState(flushBufferRatio < triggerFlushRatio);

        calculateNumBuffersLimit();

        if (poolSizeCheckInterval > 0) {
            poolSizeChecker.scheduleAtFixedRate(
                    () -> {
                        calculateNumBuffersLimit();
                        checkNeedFlushCachedBuffers();
                    },
                    poolSizeCheckInterval,
                    poolSizeCheckInterval,
                    TimeUnit.MILLISECONDS);
        }
    }

    public BufferPoolHelperImpl(
            BufferPool bufferPool,
            float bufferInMemoryRatio,
            float flushBufferRatio,
            float triggerFlushRatio) {
        this(bufferPool, bufferInMemoryRatio, flushBufferRatio, triggerFlushRatio, 0);
    }

    // ------------------------------------
    //          For Local Memory Tier
    // ------------------------------------

    @Override
    public boolean canStoreNextSegmentForMemoryTier(int bufferNumberInSegment) {
        calculateNumBuffersLimit();
        if (this.bufferNumberInSegment == -1) {
            this.bufferNumberInSegment = bufferNumberInSegment;
        }
        int currentNumberBuffer = numInMemoryBuffers.get();
        if ((currentNumberBuffer + bufferNumberInSegment) <= numInMemoryMaxBuffers) {
            numInMemoryBuffers.getAndAdd(bufferNumberInSegment);
            return true;
        } else {
            return false;
        }
        // return new Random().nextBoolean();
        // return true;
    }

    @Override
    public void decreaseRedundantBufferNumberInSegment(
            int subpartitionId, int bufferNumberInSegment) {
        int actualRequiredBufferNum = memoryTierSubpartitionRequiredBuffers[subpartitionId];
        if (actualRequiredBufferNum < bufferNumberInSegment) {
            numInMemoryBuffers.getAndAdd(actualRequiredBufferNum - bufferNumberInSegment);
        }
        memoryTierSubpartitionRequiredBuffers[subpartitionId] = 0;
    }

    void decInMemoryBuffer() {
        checkState(
                numInMemoryBuffers.decrementAndGet() >= 0,
                "Wrong number of in-mem buffers.",
                numInMemoryBuffers.get());
    }

    // ------------------------------------
    //          For Local Disk Tier
    // ------------------------------------

    @Override
    public int numPoolSize() {
        return bufferPool.getNumBuffers();
    }

    @Override
    public void registerSubpartitionTieredManager(
            TieredStoreMode.TieredType tieredType,
            CacheBufferSpillTrigger cacheBufferSpillTrigger) {
        registerSubpartitionTieredManager(
                ALL_SUBPARTITIONS_INDEX, tieredType, cacheBufferSpillTrigger);
    }

    @Override
    public MemorySegment requestMemorySegmentBlocking(
            TieredStoreMode.TieredType tieredType, boolean isInMemory) {
        return requestMemorySegmentBlocking(ALL_SUBPARTITIONS_INDEX, tieredType, isInMemory);
    }

    @Override
    public void recycleBuffer(
            MemorySegment buffer, TieredStoreMode.TieredType tieredType, boolean isInMemory) {
        recycleBuffer(ALL_SUBPARTITIONS_INDEX, buffer, tieredType, isInMemory);
    }

    @Override
    public int numCachedBuffers() {
        return numTotalCacheBuffers.get();
    }

    @Override
    public void checkNeedFlushCachedBuffers() {
        // if last flush triggering is running, return
        synchronized (this) {
            if (!isTriggeringFlush.isDone()) {
                return;
            }
            int availableBuffers = bufferPool.getNetworkBufferPoolAvailableBuffers();
            checkState(availableBuffers >= 0);
            int totalBuffers = bufferPool.getNetworkBufferPoolTotalBuffers();
            checkState(totalBuffers > 0);
            double networkBufferAvailableRatio = availableBuffers * 1.0 / totalBuffers;
            // if the buffer is enough, the cached buffers will be not flushed.
            if ((numTotalCacheBuffers.get() + numInMemoryBuffers.get() < numTriggerFlushBuffers)
                    && networkBufferAvailableRatio >= 0.2) {
                return;
            }
            isTriggeringFlush = new CompletableFuture<>();
            sortToFlushSubpartitions();
            notifySubpartitionFlush();
            isTriggeringFlush.complete(null);
        }
    }

    void incCachedBuffers(int subpartitionId, TieredStoreMode.TieredType tieredType) {
        numTotalCacheBuffers.incrementAndGet();
        checkNotNull(
                        subpartitionCachedBuffersMap.get(
                                new SubpartitionTier(subpartitionId, tieredType)))
                .incNumCachedBuffers();
    }

    void decCachedBuffers(int subpartitionId, TieredStoreMode.TieredType tieredType) {
        numTotalCacheBuffers.decrementAndGet();
        checkNotNull(
                        subpartitionCachedBuffersMap.get(
                                new SubpartitionTier(subpartitionId, tieredType)))
                .decNumCachedBuffers();
    }

    // ------------------------------------
    //             For Dfs Tier
    // ------------------------------------

    @Override
    public void registerSubpartitionTieredManager(
            int subpartitionId,
            TieredStoreMode.TieredType tieredType,
            CacheBufferSpillTrigger cacheBufferSpillTrigger) {
        SubpartitionTier subpartitionTier = new SubpartitionTier(subpartitionId, tieredType);
        SubpartitionBuffersCounter cachedBuffersCounter =
                new SubpartitionBuffersCounter(subpartitionTier, cacheBufferSpillTrigger);
        subpartitionCachedBuffersMap.put(subpartitionTier, cachedBuffersCounter);
        subpartitionBuffersCounters.add(cachedBuffersCounter);
    }

    // ------------------------------------
    //                Common
    // ------------------------------------

    @Override
    public MemorySegment requestMemorySegmentBlocking(
            int subpartitionId, TieredStoreMode.TieredType tieredType, boolean isInMemory) {
        try {
            return requestMemorySegmentFromPool(subpartitionId, tieredType, isInMemory);
            // if (!isInMemory) {
            //    return requestMemorySegmentFromPool(subpartitionId, tieredType, isInMemory);
            // }
            //
            // checkState(tieredType == TieredStoreMode.TieredType.IN_MEM);
            // if (numInMemoryBuffers.get() < numInMemoryMaxBuffers) {
            //    return requestMemorySegmentFromPool(subpartitionId, tieredType, isInMemory);
            // } else {
            //    CompletableFuture<Void> isInMemBuffersEnough = new CompletableFuture<>();
            //    requestingBuffersQueue.add(isInMemBuffersEnough);
            //    return isInMemBuffersEnough
            //            .thenApply(
            //                    ignore -> {
            //                        try {
            //                            return requestMemorySegmentFromPool(
            //                                    subpartitionId, tieredType, isInMemory);
            //                        } catch (IOException e) {
            //                            throw new RuntimeException(e);
            //                        }
            //                    })
            //            .get();
            // }
        } catch (Throwable e) {
            throw new RuntimeException("Failed to request memory segment from buffer pool.", e);
        }
    }

    @Override
    public void recycleBuffer(
            int subpartitionId,
            MemorySegment buffer,
            TieredStoreMode.TieredType tieredType,
            boolean isInMemory) {
        bufferPool.recycle(buffer);
        if (isInMemory) {
            checkState(tieredType == TieredStoreMode.TieredType.IN_MEM);
            decInMemoryBuffer();
            // if (numInMemoryBuffers.get() < numInMemoryMaxBuffers) {
            //    CompletableFuture<Void> requestingBuffer = requestingBuffersQueue.poll();
            //    if (requestingBuffer != null) {
            //        requestingBuffer.complete(null);
            //    }
            // }
        } else {
            decCachedBuffers(subpartitionId, tieredType);
        }
    }

    @Override
    public void close() {
        poolSizeChecker.shutdown();
    }

    private MemorySegment requestMemorySegmentFromPool(
            int subpartitionId, TieredStoreMode.TieredType tieredType, boolean isInMemory)
            throws IOException {
        MemorySegment requestedBuffer;
        try {
            requestedBuffer = bufferPool.requestMemorySegmentBlocking();
        } catch (Throwable throwable) {
            throw new IOException("Failed to request memory segments.", throwable);
        }
        if (isInMemory) {
            checkState(tieredType == TieredStoreMode.TieredType.IN_MEM);
            int bufferNumber = ++memoryTierSubpartitionRequiredBuffers[subpartitionId];
            if (bufferNumber > bufferNumberInSegment) {
                numInMemoryBuffers.getAndIncrement();
            }
        } else {
            incCachedBuffers(subpartitionId, tieredType);
            checkNeedFlushCachedBuffers();
        }
        return checkNotNull(requestedBuffer);
    }

    private void calculateNumBuffersLimit() {
        synchronized (this) {
            // If the buffer pool only has one buffer, the in memory buffer and cached buffer use
            // the buffer in FIFO order.
            numTotalBuffers = bufferPool.getNumBuffers();
            numInMemoryMaxBuffers = Math.max(1, (int) (numTotalBuffers * bufferInMemoryRatio));
            numStopNotifyFlushBuffers =
                    (int) (numTotalBuffers * (triggerFlushRatio - flushBufferRatio));
            numTriggerFlushBuffers = Math.max(1, (int) (numTotalBuffers * triggerFlushRatio));
        }
    }

    private void sortToFlushSubpartitions() {
        subpartitionBuffersCounters = new PriorityBlockingQueue<>(subpartitionBuffersCounters);
    }

    private void notifySubpartitionFlush() {
        int notifyTimes = subpartitionBuffersCounters.size();
        while (notifyTimes > 0) {
            SubpartitionBuffersCounter buffersCounter =
                    checkNotNull(subpartitionBuffersCounters.poll());
            buffersCounter.getNotifyFlushListener().notifyFlushCachedBuffers();
            subpartitionBuffersCounters.add(buffersCounter);
            notifyTimes--;
        }
    }

    private static class SubpartitionTier {

        private final int subpartitionId;

        private final TieredStoreMode.TieredType tieredType;

        public SubpartitionTier(int subpartition, TieredStoreMode.TieredType tieredType) {
            this.subpartitionId = subpartition;
            this.tieredType = tieredType;
        }

        public int getSubpartitionId() {
            return subpartitionId;
        }

        public TieredStoreMode.TieredType getTieredType() {
            return tieredType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SubpartitionTier that = (SubpartitionTier) o;
            return subpartitionId == that.subpartitionId && tieredType == that.tieredType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(subpartitionId, tieredType);
        }
    }

    private static class SubpartitionBuffersCounter
            implements Comparable<SubpartitionBuffersCounter> {

        SubpartitionTier subpartitionTier;

        private final CacheBufferSpillTrigger cacheBufferSpillTrigger;

        private final AtomicInteger numCachedBuffers = new AtomicInteger(0);

        public SubpartitionBuffersCounter(
                SubpartitionTier subpartitionTier,
                CacheBufferSpillTrigger cacheBufferSpillTrigger) {
            this.subpartitionTier = subpartitionTier;
            this.cacheBufferSpillTrigger = cacheBufferSpillTrigger;
        }

        public int getSubpartitionId() {
            return subpartitionTier.getSubpartitionId();
        }

        public TieredStoreMode.TieredType getTieredType() {
            return subpartitionTier.getTieredType();
        }

        int numCachedBuffers() {
            return numCachedBuffers.get();
        }

        void incNumCachedBuffers() {
            numCachedBuffers.incrementAndGet();
        }

        public CacheBufferSpillTrigger getNotifyFlushListener() {
            return cacheBufferSpillTrigger;
        }

        void decNumCachedBuffers() {
            numCachedBuffers.decrementAndGet();
        }

        @Override
        public int compareTo(SubpartitionBuffersCounter that) {
            return -1 * Integer.compare(numCachedBuffers(), that.numCachedBuffers());
        }
    }
}
