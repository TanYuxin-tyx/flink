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

import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** A scheduled service to check whether the cached buffers need to be flushed. */
public class CacheFlushManager {

    private final ScheduledExecutorService poolSizeChecker =
            Executors.newSingleThreadScheduledExecutor(
                    new ExecutorThreadFactory("tiered-store-buffer-pool-checker"));

    private final List<CacheBufferSpillTrigger> bufferSpillTriggers = new ArrayList<>();

    // TODO, make this configurable
    private int poolSizeCheckInterval = 500;

    public CacheFlushManager() {
        if (poolSizeCheckInterval > 0) {
            poolSizeChecker.scheduleAtFixedRate(
                    () -> {
                        for (CacheBufferSpillTrigger spillTrigger : bufferSpillTriggers) {
                            spillTrigger.notifyFlushCachedBuffers();
                        }
                    },
                    poolSizeCheckInterval,
                    poolSizeCheckInterval,
                    TimeUnit.MILLISECONDS);
        }
    }

    public void registerCacheSpillTrigger(CacheBufferSpillTrigger cacheBufferSpillTrigger) {
        bufferSpillTriggers.add(cacheBufferSpillTrigger);
    }

    public void close() {
        poolSizeChecker.shutdown();
    }
}
