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
import org.apache.flink.runtime.io.network.buffer.LocalBufferPool;
import org.apache.flink.runtime.io.network.partition.store.TieredStoreMode;

/**
 * The helper allocating and recycling buffer from {@link LocalBufferPool} to different tiers,
 * including Local Memory, Local Disk, Dfs.
 */
public interface BufferPoolHelper {

    // ------------------------------------
    //          For Local Memory Tier
    // ------------------------------------

    boolean canStoreNextSegmentForMemoryTier(int numSegmentSize);

    void decreaseRedundantBufferNumberInSegment(int subpartitionId, int numSegmentSize);

    // ------------------------------------
    //          For Local Disk Tier
    // ------------------------------------

    int numPoolSize();

    /**
     * When creating each tiered manager for all subpartitions, first register to this buffer pool
     * helper. If all subpartition can share the same flush listener, call this registration method.
     */
    void registerSubpartitionTieredManager(
            TieredStoreMode.TieredType tieredType, NotifyFlushListener notifyFlushListener);

    MemorySegment requestMemorySegmentBlocking(
            TieredStoreMode.TieredType tieredType, boolean isInMemory);

    void recycleBuffer(
            MemorySegment buffer, TieredStoreMode.TieredType tieredType, boolean isInMemory);

    int numCachedBuffers();

    void checkNeedFlushCachedBuffers();

    // ------------------------------------
    //             For Dfs Tier
    // ------------------------------------

    /**
     * When creating each tiered manager for each subpartition, first register to this buffer pool
     * helper. If each subpartition uses each separate flush listener, call this registration
     * method.
     */
    void registerSubpartitionTieredManager(
            int subpartitionId,
            TieredStoreMode.TieredType tieredType,
            NotifyFlushListener notifyFlushListener);

    // ------------------------------------
    //                Common
    // ------------------------------------

    MemorySegment requestMemorySegmentBlocking(
            int subpartitionId, TieredStoreMode.TieredType tieredType, boolean isInMemory);

    void recycleBuffer(
            int subpartitionId,
            MemorySegment buffer,
            TieredStoreMode.TieredType tieredType,
            boolean isInMemory);

    void close();
}
