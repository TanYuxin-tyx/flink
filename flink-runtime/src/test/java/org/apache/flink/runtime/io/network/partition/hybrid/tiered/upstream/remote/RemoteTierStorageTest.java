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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.remote;

import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.TieredStoreTestUtils;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.common.CacheFlushManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.common.TieredStoreMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.common.UpstreamTieredStoreMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.remote.RemoteTierStorage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RemoteTierStorage}. */
class RemoteTierStorageTest {

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
        RemoteTierStorage dataManager = createRemoteTier();
        assertThat(dataManager.canStoreNextSegment(0)).isTrue();
    }

    private RemoteTierStorage createRemoteTier() throws IOException {
        NetworkBufferPool networkBufferPool = new NetworkBufferPool(NUM_BUFFERS, BUFFER_SIZE);
        BufferPool bufferPool = networkBufferPool.createBufferPool(NUM_BUFFERS, NUM_BUFFERS);
        TieredStoreMemoryManager tieredStoreMemoryManager =
                new UpstreamTieredStoreMemoryManager(
                        TieredStoreTestUtils.getTierExclusiveBuffers(),
                        NUM_SUBPARTITIONS,
                        NUM_BUFFERS_TRIGGER_FLUSH_RATIO,
                        new CacheFlushManager());
        tieredStoreMemoryManager.setBufferPool(bufferPool);

        //return new RemoteTierStorage(
        //        NUM_SUBPARTITIONS,
        //        1024,
        //        tieredStoreMemoryManager,
        //        new CacheFlushManager(),
        //        false,
        //        null,
        //        new PartitionFileManagerImpl(
        //                null,
        //                null,
        //                null,
        //                null,
        //                null,
        //                NUM_SUBPARTITIONS,
        //                JobID.generate(),
        //                new ResultPartitionID(),
        //                tmpFolder.getRoot().getPath()));
        return null;
    }
}
