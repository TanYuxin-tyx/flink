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

package org.apache.flink.runtime.io.network.partition.tieredstore.upstream.remote;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.TieredStoreTestUtils;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.CacheFlushManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreMemoryManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.UpstreamTieredStoreMemoryManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.file.PartitionFileManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.file.PartitionFileManagerImpl;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.file.PartitionFileType;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.remote.RemoteCacheManager;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

import static org.apache.flink.runtime.io.network.partition.tieredstore.upstream.TieredStoreTestUtils.getTierExclusiveBuffers;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests for {@link RemoteCacheManager}. */
class RemoteDiskCacheManagerTest {

    private static final int NUM_BUFFERS = 10;

    private static final int NUM_SUBPARTITIONS = 3;

    private static final int BUFFER_SIZE = Integer.BYTES * 3;

    private static final int POOL_SIZE = 10;

    private TemporaryFolder tmpFolder;

    private PartitionFileManager partitionFileManager;

    @BeforeEach
    void setup() throws IOException {
        this.tmpFolder = TemporaryFolder.builder().build();
        tmpFolder.create();
        this.partitionFileManager =
                new PartitionFileManagerImpl(
                        null,
                        null,
                        null,
                        null,
                        null,
                        NUM_SUBPARTITIONS,
                        JobID.generate(),
                        new ResultPartitionID(),
                        tmpFolder.getRoot().getPath());
    }

    @Test
    void testStartSegmentInMultiTimes() throws Exception {
        RemoteCacheManager cacheDataManager = createRemoteCacheDataManager();
        cacheDataManager.startSegment(0, 0);
        assertThrows(IllegalStateException.class, () -> cacheDataManager.startSegment(0, 0));
    }

    @Test
    void testFinishSegmentInMultiTimes() throws Exception {
        RemoteCacheManager cacheDataManager = createRemoteCacheDataManager();
        cacheDataManager.startSegment(0, 0);
        cacheDataManager.finishSegment(0, 0);
        assertThrows(IllegalStateException.class, () -> cacheDataManager.finishSegment(0, 1));
    }

    private RemoteCacheManager createRemoteCacheDataManager() throws Exception {
        BufferPool localBufferPool =
                new NetworkBufferPool(NUM_BUFFERS, BUFFER_SIZE)
                        .createBufferPool(POOL_SIZE, POOL_SIZE);
        TieredStoreMemoryManager tieredStoreMemoryManager =
                new UpstreamTieredStoreMemoryManager(
                        localBufferPool,
                        getTierExclusiveBuffers(),
                        NUM_SUBPARTITIONS,
                        new CacheFlushManager());
        RemoteCacheManager cacheDataManager =
                new RemoteCacheManager(
                        NUM_SUBPARTITIONS,
                        BUFFER_SIZE,
                        tieredStoreMemoryManager,
                        new CacheFlushManager(),
                        null,
                        partitionFileManager.createPartitionFileWriter(
                                PartitionFileType.PRODUCER_HASH));
        cacheDataManager.setOutputMetrics(TieredStoreTestUtils.createTestingOutputMetrics());
        return cacheDataManager;
    }
}
