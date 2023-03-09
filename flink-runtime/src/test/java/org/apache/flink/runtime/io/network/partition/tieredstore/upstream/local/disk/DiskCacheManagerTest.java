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

package org.apache.flink.runtime.io.network.partition.tieredstore.upstream.local.disk;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.TieredStoreTestUtils;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.CacheFlushManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreMemoryManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.UpstreamTieredStoreMemoryManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.DiskCacheManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.RegionBufferIndexTracker;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.RegionBufferIndexTrackerImpl;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.SubpartitionDiskCacheManager;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;

import static org.apache.flink.runtime.io.network.partition.tieredstore.upstream.TieredStoreTestUtils.createTestingOutputMetrics;
import static org.apache.flink.runtime.io.network.partition.tieredstore.upstream.TieredStoreTestUtils.getTierExclusiveBuffers;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DiskCacheManager}. */
class DiskCacheManagerTest {

    private static final int NUM_BUFFERS = 10;

    private static final int NUM_SUBPARTITIONS = 3;

    private static final int POOL_SIZE = 10;

    private int bufferSize = Integer.BYTES;

    private Path dataFilePath;

    @BeforeEach
    void before(@TempDir Path tempDir) {
        this.dataFilePath = tempDir.resolve(".data");
    }

    @Test
    void testAppendMarkBufferFinished() throws Exception {
        int targetChannel = 0;
        bufferSize = Integer.BYTES * 3;
        NetworkBufferPool networkBufferPool = new NetworkBufferPool(NUM_BUFFERS, bufferSize);
        BufferPool bufferPool = networkBufferPool.createBufferPool(POOL_SIZE, POOL_SIZE);
        TieredStoreMemoryManager tieredStoreMemoryManager =
                new UpstreamTieredStoreMemoryManager(
                        bufferPool, getTierExclusiveBuffers(), NUM_SUBPARTITIONS);
        DiskCacheManager diskCacheManager = createCacheDataManager(tieredStoreMemoryManager);
        SubpartitionDiskCacheManager subpartitionDiskCacheManager =
                diskCacheManager.getSubpartitionDiskCacheManagers()[targetChannel];
        diskCacheManager.append(createRecord(0), targetChannel, Buffer.DataType.DATA_BUFFER, false);
        diskCacheManager.append(createRecord(1), targetChannel, Buffer.DataType.DATA_BUFFER, false);
        diskCacheManager.append(createRecord(2), targetChannel, Buffer.DataType.DATA_BUFFER, false);
        assertThat(subpartitionDiskCacheManager.getFinishedBufferIndex()).isEqualTo(1);
        diskCacheManager.append(createRecord(3), 0, Buffer.DataType.DATA_BUFFER, false);
        assertThat(subpartitionDiskCacheManager.getFinishedBufferIndex()).isEqualTo(1);
        diskCacheManager.append(createRecord(4), 0, Buffer.DataType.DATA_BUFFER, true);
        assertThat(subpartitionDiskCacheManager.getFinishedBufferIndex()).isEqualTo(2);
        diskCacheManager.append(createRecord(5), 0, Buffer.DataType.EVENT_BUFFER, true);
        assertThat(subpartitionDiskCacheManager.getFinishedBufferIndex()).isEqualTo(3);
        diskCacheManager.append(createRecord(6), 0, Buffer.DataType.EVENT_BUFFER, true);
        assertThat(subpartitionDiskCacheManager.getFinishedBufferIndex()).isEqualTo(4);
        diskCacheManager.append(createRecord(7), 0, Buffer.DataType.DATA_BUFFER, true);
        assertThat(subpartitionDiskCacheManager.getFinishedBufferIndex()).isEqualTo(5);
    }

    private DiskCacheManager createCacheDataManager(
            TieredStoreMemoryManager tieredStoreMemoryManager) throws Exception {
        DiskCacheManager diskCacheManager =
                new DiskCacheManager(
                        NUM_SUBPARTITIONS,
                        bufferSize,
                        tieredStoreMemoryManager,
                        new CacheFlushManager(),
                        new RegionBufferIndexTrackerImpl(NUM_SUBPARTITIONS),
                        dataFilePath,
                        null);
        diskCacheManager.setOutputMetrics(TieredStoreTestUtils.createTestingOutputMetrics());
        return diskCacheManager;
    }

    private DiskCacheManager createCacheDataManager(
            BufferPool bufferPool, RegionBufferIndexTracker regionBufferIndexTracker)
            throws Exception {
        DiskCacheManager diskCacheManager =
                new DiskCacheManager(
                        NUM_SUBPARTITIONS,
                        bufferSize,
                        new UpstreamTieredStoreMemoryManager(
                                bufferPool, getTierExclusiveBuffers(), NUM_SUBPARTITIONS),
                        new CacheFlushManager(),
                        regionBufferIndexTracker,
                        dataFilePath,
                        null);
        diskCacheManager.setOutputMetrics(createTestingOutputMetrics());
        return diskCacheManager;
    }

    private static ByteBuffer createRecord(int value) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES);
        byteBuffer.putInt(value);
        byteBuffer.flip();
        return byteBuffer;
    }
}
