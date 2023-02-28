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

import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.TieredStoreMode;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.DiskCacheManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.RegionBufferIndexTrackerImpl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link BufferPoolHelperImpl}. */
class BufferPoolHelperImplTest {

    private static final int NUM_SUBPARTITIONS = 3;

    private int bufferSize = Integer.BYTES;

    private Path dataFilePath;

    @BeforeEach
    void before(@TempDir Path tempDir) {
        this.dataFilePath = tempDir.resolve(".data");
    }

    @Test
    void testPoolSizeCheck() throws Exception {
        final int requiredBuffers = 10;
        final int maxBuffers = 100;
        CompletableFuture<Void> triggerGlobalDecision = new CompletableFuture<>();

        NetworkBufferPool networkBufferPool = new NetworkBufferPool(maxBuffers, bufferSize);
        BufferPool bufferPool = networkBufferPool.createBufferPool(requiredBuffers, maxBuffers);
        assertThat(bufferPool.getNumBuffers()).isEqualTo(maxBuffers);

        BufferPoolHelper bufferPoolHelper = new BufferPoolHelperImpl(bufferPool, 0.4f, 0.2f, 0.8f);
        createCacheDataManagerInLocalDiskTier(bufferPoolHelper);
        networkBufferPool.createBufferPool(maxBuffers - requiredBuffers, maxBuffers);
        assertThat(bufferPool.getNumBuffers()).isEqualTo(requiredBuffers);
        for (int i = 0; i < requiredBuffers; i++) {
            bufferPoolHelper.requestMemorySegmentBlocking(TieredStoreMode.TieredType.IN_LOCAL);
        }
        assertThat(triggerGlobalDecision).succeedsWithin(10, TimeUnit.SECONDS);
    }

    private void createCacheDataManagerInLocalDiskTier(BufferPoolHelper bufferPoolHelper)
            throws Exception {
        new DiskCacheManager(
                NUM_SUBPARTITIONS,
                bufferSize,
                bufferPoolHelper,
                new RegionBufferIndexTrackerImpl(NUM_SUBPARTITIONS),
                dataFilePath,
                null);
    }
}
