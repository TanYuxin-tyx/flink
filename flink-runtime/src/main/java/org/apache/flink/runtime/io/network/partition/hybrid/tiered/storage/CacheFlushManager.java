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

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FatalExitExceptionHandler;

import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A manager to check whether the cached buffers need to be flushed.
 *
 * <p>Cached buffers refer to the buffers stored in a tier that can be released by flushing them to
 * the disk or remote storage. If the requested buffers from the buffer pool reach the ratio limit,
 * {@link CacheFlushManager} will trigger a process wherein all these buffers are flushed to the
 * disk or remote storage to recycling these buffers.
 *
 * <p>Note that when initializing a tier with cached buffers, the tier should register a {@link
 * CacheBufferFlushTrigger}, as a listener to flush cached buffers.
 */
public class CacheFlushManager {
    private final float numBuffersTriggerFlushRatio;

    private final List<CacheBufferFlushTrigger> flushTriggers;

    private TieredStorageMemoryManager storageMemoryManager;

    private final ScheduledExecutorService executor =
            Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder()
                            .setNameFormat("cache flush trigger")
                            .setUncaughtExceptionHandler(FatalExitExceptionHandler.INSTANCE)
                            .build());

    public CacheFlushManager(float numBuffersTriggerFlushRatio) {
        this.numBuffersTriggerFlushRatio = numBuffersTriggerFlushRatio;
        this.flushTriggers = new ArrayList<>();

        executor.scheduleWithFixedDelay(
                this::checkNeedTriggerFlushCachedBuffers, 10, 50, TimeUnit.MILLISECONDS);
    }

    public void setup(TieredStorageMemoryManager storageMemoryManager) {
        this.storageMemoryManager = storageMemoryManager;
    }

    public void registerCacheBufferFlushTrigger(CacheBufferFlushTrigger cacheBufferFlushTrigger) {
        flushTriggers.add(cacheBufferFlushTrigger);
    }

    public void triggerFlushCachedBuffers() {
        flushTriggers.forEach(CacheBufferFlushTrigger::notifyFlushCachedBuffers);
    }

    public void checkNeedTriggerFlushCachedBuffers() {
        if (storageMemoryManager == null) {
            return;
        }

        if (TieredStorageUtils.needFlushCacheBuffers(
                storageMemoryManager, numBuffersTriggerFlushRatio)) {
            triggerFlushCachedBuffers();
        }
    }

    public int numFlushTriggers() {
        return flushTriggers.size();
    }

    public void close() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5L, TimeUnit.MINUTES)) {
                throw new TimeoutException("Timeout for shutting down the cache flush executor.");
            }
        } catch (Exception e) {
            ExceptionUtils.rethrow(e);
        }
        flushTriggers.clear();
    }
}
