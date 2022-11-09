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
import org.apache.flink.runtime.io.network.partition.store.common.BufferPoolHelper;
import org.apache.flink.runtime.io.network.partition.store.common.BufferPoolHelperImpl;
import org.apache.flink.runtime.io.network.partition.store.common.SingleTierWriter;
import org.apache.flink.runtime.io.network.partition.store.tier.dfs.DfsDataManager;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

import static org.apache.flink.runtime.io.network.partition.store.TieredStoreTestUtils.createRecord;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DfsDataManager}. */
class DfsDataManagerTest {

    private static final int NUM_BUFFERS = 10;

    private static final int NUM_SUBPARTITIONS = 3;

    private TemporaryFolder tmpFolder;

    @BeforeEach
    void setup() throws IOException {
        this.tmpFolder = TemporaryFolder.builder().build();
        tmpFolder.create();
    }

    @Test
    void testDataManagerStoreSegment() throws Exception {
        DfsDataManager dataManager = createDfsDataManager();
        assertThat(dataManager.canStoreNextSegment(0)).isTrue();
    }

    @Test
    void testDataManagerHasSegment() throws Exception {
        DfsDataManager dataManager = createDfsDataManager();
        SingleTierWriter writer = dataManager.createPartitionTierWriter();
        assertThat(dataManager.hasCurrentSegment(0, 0)).isFalse();
        writer.emit(createRecord(0), 0, Buffer.DataType.DATA_BUFFER, false, false, false, 0);
        assertThat(dataManager.hasCurrentSegment(0, 0)).isTrue();
    }

    private DfsDataManager createDfsDataManager() throws IOException {
        int bufferSize = Integer.BYTES * 3;
        int poolSize = 10;
        NetworkBufferPool networkBufferPool = new NetworkBufferPool(NUM_BUFFERS, bufferSize);
        BufferPool bufferPool = networkBufferPool.createBufferPool(poolSize, poolSize);
        BufferPoolHelper bufferPoolHelper = new BufferPoolHelperImpl(bufferPool, 0.4f, 0.2f, 0.8f);
        return new DfsDataManager(
                JobID.generate(),
                NUM_SUBPARTITIONS,
                1024,
                new ResultPartitionID(),
                bufferPoolHelper,
                false,
                tmpFolder.getRoot().getPath(),
                null);
    }
}
