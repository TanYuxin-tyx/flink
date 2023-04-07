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
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.CacheFlushManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreMemoryManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.UpstreamTieredStoreMemoryManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.file.PartitionFileManagerImpl;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.remote.RemoteTierWriter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

import static org.apache.flink.runtime.io.network.partition.tieredstore.upstream.TieredStoreTestUtils.getTierExclusiveBuffers;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RemoteTierWriter}. */
class RemoteTierContainerTest {

    private static final int NUM_BUFFERS = 10;

    private static final int NUM_SUBPARTITIONS = 3;

    private static final float NUM_BUFFERS_TRIGGER_FLUSH_RATIO = 0.6f;

    private static final int BUFFER_SIZE = Integer.BYTES * 3;

    private TemporaryFolder tmpFolder;

    @BeforeEach
    void setup() throws IOException {
        this.tmpFolder = TemporaryFolder.builder().build();
        tmpFolder.create();
    }

    @Test
    void testDataManagerStoreSegment() throws Exception {
        RemoteTierWriter dataManager = createRemoteTier();
        assertThat(dataManager.canStoreNextSegment(0)).isTrue();
    }

    private RemoteTierWriter createRemoteTier() throws IOException {
        NetworkBufferPool networkBufferPool = new NetworkBufferPool(NUM_BUFFERS, BUFFER_SIZE);
        BufferPool bufferPool = networkBufferPool.createBufferPool(NUM_BUFFERS, NUM_BUFFERS);
        TieredStoreMemoryManager tieredStoreMemoryManager =
                new UpstreamTieredStoreMemoryManager(
                        getTierExclusiveBuffers(),
                        NUM_SUBPARTITIONS,
                        NUM_BUFFERS_TRIGGER_FLUSH_RATIO,
                        new CacheFlushManager());
        tieredStoreMemoryManager.setBufferPool(bufferPool);

        return new RemoteTierWriter(
                NUM_SUBPARTITIONS,
                1024,
                tieredStoreMemoryManager,
                new CacheFlushManager(),
                false,
                null,
                new PartitionFileManagerImpl(
                        null,
                        null,
                        null,
                        null,
                        null,
                        NUM_SUBPARTITIONS,
                        JobID.generate(),
                        new ResultPartitionID(),
                        tmpFolder.getRoot().getPath()));
    }
}
