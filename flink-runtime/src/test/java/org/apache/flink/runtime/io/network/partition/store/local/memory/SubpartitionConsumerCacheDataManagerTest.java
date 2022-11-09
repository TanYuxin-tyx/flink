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

package org.apache.flink.runtime.io.network.partition.store.local.memory;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.ReadOnlySlicedNetworkBuffer;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.runtime.io.network.partition.store.common.BufferContext;
import org.apache.flink.runtime.io.network.partition.store.common.ConsumerId;
import org.apache.flink.runtime.io.network.partition.store.tier.local.file.CacheDataManagerOperation;
import org.apache.flink.runtime.io.network.partition.store.tier.local.file.SubpartitionConsumerCacheDataManager;

import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.flink.runtime.io.network.partition.store.TieredStoreTestUtils.createBuffer;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SubpartitionConsumerCacheDataManager}. */
@SuppressWarnings("FieldAccessNotGuarded")
class SubpartitionConsumerCacheDataManagerTest {

    private static final int BUFFER_SIZE = Long.BYTES;

    private static final int SUBPARTITION_ID = 0;

    @Test
    void testPeekNextToConsumeDataTypeNotMeetBufferIndexToConsume() {
        TestingCacheDataManagerOperation memoryDataManagerOperation =
                TestingCacheDataManagerOperation.builder().build();
        SubpartitionConsumerCacheDataManager subpartitionConsumerCacheDataManager =
                createSubpartitionConsumerMemoryDataManager(memoryDataManagerOperation);

        subpartitionConsumerCacheDataManager.addBuffer(createBufferContext(0, false));
        assertThat(subpartitionConsumerCacheDataManager.peekNextToConsumeDataType(1, new ArrayDeque<>()))
                .isEqualTo(Buffer.DataType.NONE);
    }

    @Test
    void testPeekNextToConsumeDataTypeTrimHeadingReleasedBuffers() {
        TestingCacheDataManagerOperation memoryDataManagerOperation =
                TestingCacheDataManagerOperation.builder().build();
        SubpartitionConsumerCacheDataManager subpartitionConsumerCacheDataManager =
                createSubpartitionConsumerMemoryDataManager(memoryDataManagerOperation);

        BufferContext buffer1 = createBufferContext(0, false);
        BufferContext buffer2 = createBufferContext(1, false);
        subpartitionConsumerCacheDataManager.addBuffer(buffer1);
        subpartitionConsumerCacheDataManager.addBuffer(buffer2);
        subpartitionConsumerCacheDataManager.addBuffer(createBufferContext(2, true));

        buffer1.release();
        buffer2.release();

        assertThat(subpartitionConsumerCacheDataManager.peekNextToConsumeDataType(2, new ArrayDeque<>()))
                .isEqualTo(Buffer.DataType.EVENT_BUFFER);
    }

    @Test
    void testConsumeBufferFirstUnConsumedBufferIndexNotMeetNextToConsume() {
        TestingCacheDataManagerOperation memoryDataManagerOperation =
                TestingCacheDataManagerOperation.builder().build();
        SubpartitionConsumerCacheDataManager subpartitionConsumerCacheDataManager =
                createSubpartitionConsumerMemoryDataManager(memoryDataManagerOperation);

        subpartitionConsumerCacheDataManager.addBuffer(createBufferContext(0, false));
        assertThat(subpartitionConsumerCacheDataManager.consumeBuffer(1, new ArrayDeque<>())).isNotPresent();
    }

    @Test
    void testConsumeBufferTrimHeadingReleasedBuffers() {
        TestingCacheDataManagerOperation memoryDataManagerOperation =
                TestingCacheDataManagerOperation.builder().build();
        SubpartitionConsumerCacheDataManager subpartitionConsumerCacheDataManager =
                createSubpartitionConsumerMemoryDataManager(memoryDataManagerOperation);

        BufferContext buffer1 = createBufferContext(0, false);
        BufferContext buffer2 = createBufferContext(1, false);
        subpartitionConsumerCacheDataManager.addBuffer(buffer1);
        subpartitionConsumerCacheDataManager.addBuffer(buffer2);
        subpartitionConsumerCacheDataManager.addBuffer(createBufferContext(2, true));

        buffer1.release();
        buffer2.release();

        assertThat(subpartitionConsumerCacheDataManager.consumeBuffer(2, new ArrayDeque<>())).isPresent();
    }

    @Test
    void testConsumeBufferReturnSlice() {
        TestingCacheDataManagerOperation memoryDataManagerOperation =
                TestingCacheDataManagerOperation.builder().build();
        SubpartitionConsumerCacheDataManager subpartitionConsumerCacheDataManager =
                createSubpartitionConsumerMemoryDataManager(memoryDataManagerOperation);

        subpartitionConsumerCacheDataManager.addBuffer(createBufferContext(0, false));

        Optional<ResultSubpartition.BufferAndBacklog> bufferOpt =
                subpartitionConsumerCacheDataManager.consumeBuffer(0, new ArrayDeque<>());
        assertThat(bufferOpt)
                .hasValueSatisfying(
                        (bufferAndBacklog ->
                                assertThat(bufferAndBacklog.buffer())
                                        .isInstanceOf(ReadOnlySlicedNetworkBuffer.class)));
    }

    @Test
    void testAddBuffer() {
        TestingCacheDataManagerOperation memoryDataManagerOperation =
                TestingCacheDataManagerOperation.builder().build();
        SubpartitionConsumerCacheDataManager subpartitionConsumerCacheDataManager =
                createSubpartitionConsumerMemoryDataManager(memoryDataManagerOperation);
        ArrayDeque<BufferContext> initialBuffers = new ArrayDeque<>();
        initialBuffers.add(createBufferContext(0, false));
        initialBuffers.add(createBufferContext(1, false));
        subpartitionConsumerCacheDataManager.addInitialBuffers(initialBuffers);
        subpartitionConsumerCacheDataManager.addBuffer(createBufferContext(2, true));

        assertThat(subpartitionConsumerCacheDataManager.consumeBuffer(0, new ArrayDeque<>()))
                .hasValueSatisfying(
                        bufferAndBacklog -> {
                            assertThat(bufferAndBacklog.getSequenceNumber()).isEqualTo(0);
                            assertThat(bufferAndBacklog.buffer().getDataType())
                                    .isEqualTo(Buffer.DataType.DATA_BUFFER);
                        });
        assertThat(subpartitionConsumerCacheDataManager.consumeBuffer(1, new ArrayDeque<>()))
                .hasValueSatisfying(
                        bufferAndBacklog -> {
                            assertThat(bufferAndBacklog.getSequenceNumber()).isEqualTo(1);
                            assertThat(bufferAndBacklog.buffer().getDataType())
                                    .isEqualTo(Buffer.DataType.DATA_BUFFER);
                        });
        assertThat(subpartitionConsumerCacheDataManager.consumeBuffer(2, new ArrayDeque<>()))
                .hasValueSatisfying(
                        bufferAndBacklog -> {
                            assertThat(bufferAndBacklog.getSequenceNumber()).isEqualTo(2);
                            assertThat(bufferAndBacklog.buffer().getDataType())
                                    .isEqualTo(Buffer.DataType.EVENT_BUFFER);
                        });
    }

    @Test
    void testRelease() {
        CompletableFuture<ConsumerId> consumerReleasedFuture = new CompletableFuture<>();
        TestingCacheDataManagerOperation memoryDataManagerOperation =
                TestingCacheDataManagerOperation.builder()
                        .setOnConsumerReleasedBiConsumer(
                                (subpartitionId, consumerId) -> {
                                    consumerReleasedFuture.complete(consumerId);
                                })
                        .build();
        ConsumerId consumerId = ConsumerId.newId(null);
        SubpartitionConsumerCacheDataManager subpartitionConsumerCacheDataManager =
                createSubpartitionConsumerMemoryDataManager(consumerId, memoryDataManagerOperation);
        subpartitionConsumerCacheDataManager.releaseDataView();
        assertThat(consumerReleasedFuture).isCompletedWithValue(consumerId);
    }

    private static BufferContext createBufferContext(int bufferIndex, boolean isEvent) {
        return new BufferContext(
                createBuffer(BUFFER_SIZE, isEvent), bufferIndex, SUBPARTITION_ID, false);
    }

    private SubpartitionConsumerCacheDataManager createSubpartitionConsumerMemoryDataManager(
            CacheDataManagerOperation cacheDataManagerOperation) {
        return createSubpartitionConsumerMemoryDataManager(
                ConsumerId.DEFAULT, cacheDataManagerOperation);
    }

    private SubpartitionConsumerCacheDataManager createSubpartitionConsumerMemoryDataManager(
            ConsumerId consumerId, CacheDataManagerOperation cacheDataManagerOperation) {
        return new SubpartitionConsumerCacheDataManager(
                new ReentrantLock(),
                new ReentrantLock(),
                // consumerMemoryDataManager is a member of subpartitionMemoryDataManager, using a
                // fixed subpartition id is enough.
                SUBPARTITION_ID,
                consumerId,
                cacheDataManagerOperation);
    }
}
