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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.todo;

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManagerImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file.ProducerMergePartitionFile;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.remote.RemoteCacheManager;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests for {@link RemoteCacheManager}. */
class RemoteDiskCacheManagerTest {

    private static final int NUM_BUFFERS = 10;

    private static final int NUM_SUBPARTITIONS = 3;

    private static final int BUFFER_SIZE = Integer.BYTES * 3;

    private static final int POOL_SIZE = 10;

    private TemporaryFolder tmpFolder;

    @BeforeEach
    void setup() throws IOException {
        this.tmpFolder = TemporaryFolder.builder().build();
        tmpFolder.create();
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

    private RemoteCacheManager createRemoteCacheDataManager() {
        RemoteCacheManager cacheDataManager =
                new RemoteCacheManager(
                        NUM_SUBPARTITIONS,
                        new TieredStorageMemoryManagerImpl(0.5f, false),
                        ProducerMergePartitionFile.createPartitionFileWriter(
                                tmpFolder.getRoot().toPath(),
                                new TestingRegionBufferIndexTracker.Builder().build()));
        return cacheDataManager;
    }
}
