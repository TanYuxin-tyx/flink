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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.common;

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.common.StorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.common.TieredStoreUtils;
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
 * A service to check whether the cached buffers need to be flushed. When initializing the tier
 * manager, it will register a {@link CacheBufferFlushTrigger}, as a listener to flush cached
 * buffers. When the buffers should be flushed, the {@link CacheFlushManager} will tell the
 * listeners to trigger flushing process.
 */
public class CacheFlushManager {

    private final float numBuffersTriggerFlushRatio;

    private final List<CacheBufferFlushTrigger> spillTriggers;

    private StorageMemoryManager storageMemoryManager;

    private final ScheduledExecutorService executor =
            Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder()
                            .setNameFormat("cache flush trigger")
                            .setUncaughtExceptionHandler(FatalExitExceptionHandler.INSTANCE)
                            .build());

    public CacheFlushManager(float numBuffersTriggerFlushRatio) {
        this.numBuffersTriggerFlushRatio = numBuffersTriggerFlushRatio;
        this.spillTriggers = new ArrayList<>();

        executor.scheduleWithFixedDelay(
                this::checkNeedTriggerFlushCachedBuffers, 10, 50, TimeUnit.MILLISECONDS);
    }

    public void setup(StorageMemoryManager storageMemoryManager) {
        this.storageMemoryManager = storageMemoryManager;
    }

    public void registerCacheBufferFlushTrigger(CacheBufferFlushTrigger cacheBufferFlushTrigger) {
        spillTriggers.add(cacheBufferFlushTrigger);
    }

    public void triggerFlushCachedBuffers() {
        spillTriggers.forEach(CacheBufferFlushTrigger::notifyFlushCachedBuffers);
    }

    public void checkNeedTriggerFlushCachedBuffers() {
        if (storageMemoryManager == null) {
            return;
        }

        if (TieredStoreUtils.needFlushCacheBuffers(
                storageMemoryManager, numBuffersTriggerFlushRatio)) {
            triggerFlushCachedBuffers();
        }
    }

    public float numBuffersTriggerFlushRatio() {
        return numBuffersTriggerFlushRatio;
    }

    public void close() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5L, TimeUnit.MINUTES)) {
                throw new TimeoutException("Shutdown cache flusher thread timeout.");
            }
        } catch (Exception e) {
            ExceptionUtils.rethrow(e);
        }
        spillTriggers.clear();
    }
}
