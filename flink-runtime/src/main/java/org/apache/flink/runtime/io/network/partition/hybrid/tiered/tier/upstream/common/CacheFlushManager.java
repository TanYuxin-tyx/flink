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

import java.util.ArrayList;
import java.util.List;

/**
 * A service to check whether the cached buffers need to be flushed. When initializing the tier
 * manager, it will register a {@link CacheBufferFlushTrigger}, as a listener to flush cached
 * buffers. When the buffers should be flushed, the {@link CacheFlushManager} will tell the
 * listeners to trigger flushing process.
 */
public class CacheFlushManager {

    private final List<CacheBufferFlushTrigger> spillTriggers;

    public CacheFlushManager() {
        this.spillTriggers = new ArrayList<>();
    }

    public void registerCacheBufferFlushTrigger(CacheBufferFlushTrigger cacheBufferFlushTrigger) {
        spillTriggers.add(cacheBufferFlushTrigger);
    }

    public void triggerFlushCachedBuffers() {
        spillTriggers.forEach(CacheBufferFlushTrigger::notifyFlushCachedBuffers);
    }

    public void close() {
        spillTriggers.clear();
    }
}
