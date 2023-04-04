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
import org.apache.flink.runtime.io.network.buffer.LocalBufferPool;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.TieredStoreMode;

/**
 * The helper allocating and recycling buffer from {@link LocalBufferPool} to different tiers,
 * including Local Memory, Local Disk, Dfs.
 */
public interface TieredStoreMemoryManager {

    int numAvailableBuffers(TieredStoreMode.TierType tierType);

    int numRequestedBuffers();

    int numTotalBuffers();

    float numBuffersTriggerFlushRatio();

    MemorySegment requestMemorySegmentBlocking(TieredStoreMode.TierType tierType);

    void recycleBuffer(MemorySegment memorySegment, TieredStoreMode.TierType tierType);

    void incNumRequestedBuffer(TieredStoreMode.TierType tierType);

    void decNumRequestedBuffer(TieredStoreMode.TierType tierType);

    void checkNeedTriggerFlushCachedBuffers();

    void close();

    void release();
}
