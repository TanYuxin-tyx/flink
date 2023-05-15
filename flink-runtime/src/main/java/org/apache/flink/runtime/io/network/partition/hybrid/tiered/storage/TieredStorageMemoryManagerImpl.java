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

    /** Initial delay before checking flush. */
    public static final int DEFAULT_CHECK_FLUSH_INITIAL_DELAY_MS = 10;

    /** Check flush period. */
    public static final int DEFAULT_CHECK_FLUSH_PERIOD_DURATION_MS = 50;

    /** The tiered storage memory specs of each memory user owner. */
    private final Map<Object, TieredStorageMemorySpec> tieredMemorySpecs;

    /** The registered callbacks to flush the buffers in the registered tiered storages. */
    private final List<Runnable> bufferFlushCallbacks;

    /** The buffer pool usage ratio of triggering the registered storages to flush buffers. */
    private final float numBuffersTriggerFlushRatio;

    /**
     * Indicate whether to start the buffer flush checker thread. If the memory manager is used in
     * downstream, the field will be false because no buffer flush checker is needed.
     */
    private final boolean shouldStartBufferFlushChecker;

    /** The number of requested buffers from {@link BufferPool}. */
    private final AtomicInteger numRequestedBuffers;

    /** A thread to check whether to flush buffers in each tiered storage. */
    private ScheduledExecutorService executor;

    /** The total number of guaranteed buffers for all tiered storages. */
    private int numTotalGuaranteedBuffers;

    /** The buffer pool where the buffer is requested or recyceld. */
    private BufferPool bufferPool;

    /**
     * Indicate whether the {@link TieredStorageMemoryManagerImpl} is in running state. Before
     * setting up, this field is false.
     *
     * <p>Note that before requesting buffers or getting the maximum allowed buffers, this running
     * state should be checked.
     */
    private boolean isRunning;

    /**
     * The constructor of the {@link TieredStorageMemoryManagerImpl}.
     *
     * @param numBuffersTriggerFlushRatio the buffer pool usage ratio of triggering each tiered
     *     storage to flush buffers
     * @param shouldStartBufferFlushChecker indicate whether to start the buffer flushing checker
     *     thread
     */
    public TieredStorageMemoryManagerImpl(
            float numBuffersTriggerFlushRatio, boolean shouldStartBufferFlushChecker) {
        this.numBuffersTriggerFlushRatio = numBuffersTriggerFlushRatio;
        this.shouldStartBufferFlushChecker = shouldStartBufferFlushChecker;
        this.tieredMemorySpecs = new HashMap<>();
        this.numRequestedBuffers = new AtomicInteger(0);
        this.bufferFlushCallbacks = new ArrayList<>();
        this.isRunning = false;
    }

    @Override
    public void setup(BufferPool bufferPool, List<TieredStorageMemorySpec> storageMemorySpecs) {
        this.bufferPool = bufferPool;
        for (TieredStorageMemorySpec memorySpec : storageMemorySpecs) {
            checkState(
                    !tieredMemorySpecs.containsKey(memorySpec.getOwner()),
                    "Duplicated memory spec.");
            tieredMemorySpecs.put(memorySpec.getOwner(), memorySpec);
            numTotalGuaranteedBuffers += memorySpec.getNumGuaranteedBuffers();
        }

        if (shouldStartBufferFlushChecker) {
            this.executor =
                    Executors.newSingleThreadScheduledExecutor(
                            new ThreadFactoryBuilder()
                                    .setNameFormat("buffer flush checker")
                                    .setUncaughtExceptionHandler(FatalExitExceptionHandler.INSTANCE)
                                    .build());
            this.executor.scheduleWithFixedDelay(
                    this::checkShouldFlushCachedBuffers,
                    DEFAULT_CHECK_FLUSH_INITIAL_DELAY_MS,
                    DEFAULT_CHECK_FLUSH_PERIOD_DURATION_MS,
                    TimeUnit.MILLISECONDS);
        }

        this.isRunning = true;
    }

    @Override
    public void registerBufferFlushCallback(Runnable userBufferFlushCallBack) {
        bufferFlushCallbacks.add(userBufferFlushCallBack);
    }

    /**
     * Request a {@link BufferBuilder} instance from {@link BufferPool} for a specific owner. The
     * {@link TieredStorageMemoryManagerImpl} will not check whether a buffer can be requested and
     * only record the total number of requested buffers. If the buffers in the {@link BufferPool}
     * is not enough, this will trigger each tiered storage to flush buffers as much as possible.
     *
     * @return the requested buffer
     */
    @Override
    public BufferBuilder requestBufferBlocking() {
        checkIsRunning();

        MemorySegment requestedBuffer = null;
        try {
            requestedBuffer = bufferPool.requestMemorySegmentBlocking();
        } catch (Throwable throwable) {
            ExceptionUtils.rethrow(throwable, "Failed to request memory segments.");
        }
        numRequestedBuffers.incrementAndGet();
        checkShouldFlushCachedBuffers();
        return new BufferBuilder(checkNotNull(requestedBuffer), this::recycleBuffer);
    }

    @Override
    public int getMaxAllowedBuffers(Object owner) {
        checkIsRunning();

        TieredStorageMemorySpec ownerMemorySpec = checkNotNull(tieredMemorySpecs.get(owner));
        if (ownerMemorySpec.isMemoryReleasable()) {
            return Integer.MAX_VALUE;
        } else {
            int ownerGuaranteedBuffers = ownerMemorySpec.getNumGuaranteedBuffers();
            return bufferPool.getNumBuffers()
                    - numRequestedBuffers.get()
                    - numTotalGuaranteedBuffers
                    + ownerGuaranteedBuffers;
        }
    }

    @Override
    public void release() {
        checkState(numRequestedBuffers.get() == 0, "Leaking buffers.");
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(5L, TimeUnit.MINUTES)) {
                    throw new TimeoutException(
                            "Timeout for shutting down the cache flush executor.");
                }
            } catch (Exception e) {
                ExceptionUtils.rethrow(e);
            }
        }
    }

    private void checkShouldFlushCachedBuffers() {
        if (shouldFlushBuffers()) {
            bufferFlushCallbacks.forEach(Runnable::run);
        }
    }

    private boolean shouldFlushBuffers() {
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

    private void checkIsRunning() {
        checkState(isRunning, "The memory manager is not in running state.");
    }
}
