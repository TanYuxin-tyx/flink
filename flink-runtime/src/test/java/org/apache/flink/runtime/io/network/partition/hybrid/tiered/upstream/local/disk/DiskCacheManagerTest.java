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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.local.disk;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.common.StorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.common.CacheFlushManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.common.file.PartitionFileManagerImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.local.disk.DiskCacheManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.local.disk.RegionBufferIndexTrackerImpl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;

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

    private DiskCacheManager createCacheDataManager(StorageMemoryManager storageMemoryManager)
            throws Exception {
        DiskCacheManager diskCacheManager =
                new DiskCacheManager(
                        0,
                        NUM_SUBPARTITIONS,
                        bufferSize,
                        storageMemoryManager,
                        new CacheFlushManager(0.5f),
                        null,
                        new PartitionFileManagerImpl(
                                dataFilePath,
                                new RegionBufferIndexTrackerImpl(NUM_SUBPARTITIONS),
                                null,
                                null,
                                null,
                                NUM_SUBPARTITIONS,
                                JobID.generate(),
                                new ResultPartitionID(),
                                null));
        return diskCacheManager;
    }

    private static ByteBuffer createRecord(int value) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES);
        byteBuffer.putInt(value);
        byteBuffer.flip();
        return byteBuffer;
    }
}
