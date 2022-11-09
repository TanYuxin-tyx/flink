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

/**
 * Notify specific subpartition's tiered manager to flush the cached buffers to release buffers. The
 * notification is sent from {@link UpstreamTieredStoreMemoryManager}. Each tier of each
 * subpartition corresponds to a {@link CacheBufferFlushTrigger}.
 */
public interface CacheBufferFlushTrigger {
    void notifyFlushCachedBuffers();
}
