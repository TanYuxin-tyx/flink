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

package org.apache.flink.runtime.io.network.partition.store.dfs;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.store.TieredStoreTestUtils;
import org.apache.flink.runtime.io.network.partition.store.common.BufferPoolHelper;
import org.apache.flink.runtime.io.network.partition.store.common.BufferPoolHelperImpl;
import org.apache.flink.runtime.io.network.partition.store.tier.dfs.DfsCacheDataManager;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

import static org.apache.flink.runtime.io.network.partition.store.TieredStoreTestUtils.createRecord;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests for {@link DfsCacheDataManager}. */
class DfsCacheDataManagerTest {

    private static final int NUM_BUFFERS = 10;

    private static final int NUM_SUBPARTITIONS = 3;

    private final int poolSize = 10;

    private int bufferSize = Integer.BYTES;

    private TemporaryFolder tmpFolder;

    @BeforeEach
    void setup() throws IOException {
        this.tmpFolder = TemporaryFolder.builder().build();
        tmpFolder.create();
    }

    @Test
    void testAppendMarkBufferFinished() throws Exception {
        bufferSize = Integer.BYTES * 3;
        NetworkBufferPool networkBufferPool = new NetworkBufferPool(NUM_BUFFERS, bufferSize);
        BufferPool bufferPool = networkBufferPool.createBufferPool(poolSize, poolSize);
        BufferPoolHelper bufferPoolHelper = new BufferPoolHelperImpl(bufferPool, 0.4f, 0.2f, 0.8f);
        DfsCacheDataManager cacheDataManager = createDfsCacheDataManager(bufferPoolHelper);

        cacheDataManager.append(createRecord(0), 0, Buffer.DataType.DATA_BUFFER, false);
        cacheDataManager.append(createRecord(1), 0, Buffer.DataType.DATA_BUFFER, false);
        cacheDataManager.append(createRecord(2), 0, Buffer.DataType.DATA_BUFFER, false);
        assertThat(bufferPoolHelper.numCachedBuffers()).isEqualTo(1);

        cacheDataManager.append(createRecord(3), 0, Buffer.DataType.DATA_BUFFER, false);
        assertThat(bufferPoolHelper.numCachedBuffers()).isEqualTo(2);
    }

    @Test
    void testStartSegment() throws Exception {
        bufferSize = Integer.BYTES * 3;
        NetworkBufferPool networkBufferPool = new NetworkBufferPool(NUM_BUFFERS, bufferSize);
        BufferPool bufferPool = networkBufferPool.createBufferPool(poolSize, poolSize);
        BufferPoolHelper bufferPoolHelper = new BufferPoolHelperImpl(bufferPool, 0.4f, 0.2f, 0.8f);
        DfsCacheDataManager cacheDataManager = createDfsCacheDataManager(bufferPoolHelper);

        cacheDataManager.startSegment(0, 0);
        assertThrows(IllegalStateException.class, () -> cacheDataManager.startSegment(0, 0));
    }

    @Test
    void testFinishSegment() throws Exception {
        bufferSize = Integer.BYTES * 3;
        NetworkBufferPool networkBufferPool = new NetworkBufferPool(NUM_BUFFERS, bufferSize);
        BufferPool bufferPool = networkBufferPool.createBufferPool(poolSize, poolSize);
        BufferPoolHelper bufferPoolHelper = new BufferPoolHelperImpl(bufferPool, 0.4f, 0.2f, 0.8f);
        DfsCacheDataManager cacheDataManager = createDfsCacheDataManager(bufferPoolHelper);

        cacheDataManager.startSegment(0, 0);
        cacheDataManager.finishSegment(0, 0);
        assertThrows(IllegalStateException.class, () -> cacheDataManager.finishSegment(0, 0));
    }

    private DfsCacheDataManager createDfsCacheDataManager(BufferPoolHelper bufferPoolHelper)
            throws Exception {
        DfsCacheDataManager cacheDataManager =
                new DfsCacheDataManager(
                        JobID.generate(),
                        new ResultPartitionID(),
                        NUM_SUBPARTITIONS,
                        bufferSize,
                        false,
                        tmpFolder.getRoot().getPath(),
                        bufferPoolHelper,
                        null);
        cacheDataManager.setOutputMetrics(TieredStoreTestUtils.createTestingOutputMetrics());
        return cacheDataManager;
    }
}
