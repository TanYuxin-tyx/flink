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
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.BufferPoolHelper;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.BufferPoolHelperNewImpl;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierWriter;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.remote.RemoteTier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

import static org.apache.flink.runtime.io.network.partition.tieredstore.upstream.TieredStoreTestUtils.createRecord;
import static org.apache.flink.runtime.io.network.partition.tieredstore.upstream.TieredStoreTestUtils.getTierExclusiveBuffers;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RemoteTier}. */
class RemoteTierTest {

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
        RemoteTier dataManager = createDfsDataManager();
        assertThat(dataManager.canStoreNextSegment(0)).isTrue();
    }

    @Test
    void testDataManagerHasSegment() throws Exception {
        RemoteTier dataManager = createDfsDataManager();
        TierWriter writer = dataManager.createPartitionTierWriter();
        assertThat(dataManager.hasCurrentSegment(0, 0)).isFalse();
        writer.emit(createRecord(0), 0, Buffer.DataType.DATA_BUFFER, false, false, false, 0);
        assertThat(dataManager.hasCurrentSegment(0, 0)).isTrue();
    }

    private RemoteTier createDfsDataManager() throws IOException {
        int bufferSize = Integer.BYTES * 3;
        int poolSize = 10;
        NetworkBufferPool networkBufferPool = new NetworkBufferPool(NUM_BUFFERS, bufferSize);
        BufferPool bufferPool = networkBufferPool.createBufferPool(poolSize, poolSize);
        BufferPoolHelper bufferPoolHelper =
                new BufferPoolHelperNewImpl(
                        bufferPool, getTierExclusiveBuffers(), NUM_SUBPARTITIONS);
        return new RemoteTier(
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