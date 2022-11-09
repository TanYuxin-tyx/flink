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
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.store.TieredStoreTestUtils;
import org.apache.flink.runtime.io.network.partition.store.common.BufferIndexAndChannel;
import org.apache.flink.runtime.io.network.partition.store.common.BufferPoolHelper;
import org.apache.flink.runtime.io.network.partition.store.common.BufferPoolHelperImpl;
import org.apache.flink.runtime.io.network.partition.store.common.ConsumerId;
import org.apache.flink.runtime.io.network.partition.store.tier.local.file.CacheDataManager;
import org.apache.flink.runtime.io.network.partition.store.tier.local.file.RegionBufferIndexTracker;
import org.apache.flink.runtime.io.network.partition.store.tier.local.file.RegionBufferIndexTrackerImpl;
import org.apache.flink.runtime.io.network.partition.store.tier.local.file.TsSpillingStrategy;
import org.apache.flink.runtime.io.network.partition.store.tier.local.file.TsSpillingStrategy.Decision;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.runtime.io.network.partition.store.TieredStoreTestUtils.createTestingOutputMetrics;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link CacheDataManager}. */
class CacheDataManagerTest {
    private static final int NUM_BUFFERS = 10;

    private static final int NUM_SUBPARTITIONS = 3;

    private int poolSize = 10;

    private int bufferSize = Integer.BYTES;

    private Path dataFilePath;

    @BeforeEach
    void before(@TempDir Path tempDir) {
        this.dataFilePath = tempDir.resolve(".data");
    }

    @Test
    void testAppendMarkBufferFinished() throws Exception {
        AtomicInteger finishedBuffers = new AtomicInteger(0);
        TsSpillingStrategy spillingStrategy =
                TestingSpillingStrategy.builder()
                        .setOnBufferFinishedFunction(
                                (numTotalUnSpillBuffers, currentPoolSize) -> {
                                    finishedBuffers.incrementAndGet();
                                    return Optional.of(TsSpillingStrategy.Decision.NO_ACTION);
                                })
                        .build();
        bufferSize = Integer.BYTES * 3;
        NetworkBufferPool networkBufferPool = new NetworkBufferPool(NUM_BUFFERS, bufferSize);
        BufferPool bufferPool = networkBufferPool.createBufferPool(poolSize, poolSize);
        BufferPoolHelper bufferPoolHelper = new BufferPoolHelperImpl(bufferPool, 0.4f, 0.2f, 0.8f);
        CacheDataManager cacheDataManager =
                createCacheDataManager(spillingStrategy, bufferPoolHelper);

        cacheDataManager.append(createRecord(0), 0, Buffer.DataType.DATA_BUFFER, false);
        cacheDataManager.append(createRecord(1), 0, Buffer.DataType.DATA_BUFFER, false);
        cacheDataManager.append(createRecord(2), 0, Buffer.DataType.DATA_BUFFER, false);
        assertThat(finishedBuffers).hasValue(1);

        cacheDataManager.append(createRecord(3), 0, Buffer.DataType.DATA_BUFFER, false);
        assertThat(finishedBuffers).hasValue(1);
        cacheDataManager.append(createRecord(4), 0, Buffer.DataType.DATA_BUFFER, true);
        assertThat(finishedBuffers).hasValue(2);
        cacheDataManager.append(createRecord(5), 0, Buffer.DataType.EVENT_BUFFER, true);
        assertThat(finishedBuffers).hasValue(3);
        cacheDataManager.append(createRecord(6), 0, Buffer.DataType.EVENT_BUFFER, true);
        assertThat(finishedBuffers).hasValue(4);
        cacheDataManager.append(createRecord(7), 0, Buffer.DataType.DATA_BUFFER, true);
        assertThat(finishedBuffers).hasValue(5);
    }

    @Test
    void testHandleDecision() throws Exception {
        final int targetSubpartition = 0;
        final int numFinishedBufferToTriggerDecision = 4;
        List<BufferIndexAndChannel> toSpill =
                TieredStoreTestUtils.createBufferIndexAndChannelsList(targetSubpartition, 0, 1, 2);
        List<BufferIndexAndChannel> toRelease =
                TieredStoreTestUtils.createBufferIndexAndChannelsList(targetSubpartition, 2, 3);
        TsSpillingStrategy spillingStrategy =
                TestingSpillingStrategy.builder()
                        .setOnBufferFinishedFunction(
                                (numFinishedBuffers, poolSize) -> {
                                    if (numFinishedBuffers < numFinishedBufferToTriggerDecision) {
                                        return Optional.of(Decision.NO_ACTION);
                                    }
                                    return Optional.of(
                                            Decision.builder()
                                                    .addBufferToSpill(targetSubpartition, toSpill)
                                                    .addBufferToRelease(
                                                            targetSubpartition, toRelease)
                                                    .build());
                                })
                        .build();
        CompletableFuture<List<RegionBufferIndexTracker.SpilledBuffer>> spilledFuture =
                new CompletableFuture<>();
        CompletableFuture<Integer> readableFuture = new CompletableFuture<>();
        TestingRegionBufferIndexTracker dataIndex =
                TestingRegionBufferIndexTracker.builder()
                        .setAddBuffersConsumer(spilledFuture::complete)
                        .setMarkBufferReadableConsumer(
                                (subpartitionId, bufferIndex) ->
                                        readableFuture.complete(bufferIndex))
                        .build();
        CacheDataManager cacheDataManager = createCacheDataManager(spillingStrategy, dataIndex);
        for (int i = 0; i < 4; i++) {
            cacheDataManager.append(
                    createRecord(i), targetSubpartition, Buffer.DataType.DATA_BUFFER, false);
        }

        assertThat(spilledFuture).succeedsWithin(10, TimeUnit.SECONDS);
        assertThat(readableFuture).succeedsWithin(10, TimeUnit.SECONDS);
        assertThat(readableFuture).isCompletedWithValue(2);
        assertThat(cacheDataManager.getNumTotalUnSpillBuffers()).isEqualTo(1);
    }

    @Test
    void testResultPartitionClosed() throws Exception {
        CompletableFuture<Void> resultPartitionReleaseFuture = new CompletableFuture<>();
        TsSpillingStrategy spillingStrategy =
                TestingSpillingStrategy.builder()
                        .setOnResultPartitionClosedFunction(
                                (ignore) -> {
                                    resultPartitionReleaseFuture.complete(null);
                                    return Decision.NO_ACTION;
                                })
                        .build();
        CacheDataManager cacheDataManager = createCacheDataManager(spillingStrategy);
        cacheDataManager.close();
        assertThat(resultPartitionReleaseFuture).isCompleted();
    }

    @Test
    void testSubpartitionConsumerRelease() throws Exception {
        TsSpillingStrategy spillingStrategy = TestingSpillingStrategy.builder().build();
        CacheDataManager cacheDataManager = createCacheDataManager(spillingStrategy);
        cacheDataManager.registerNewConsumer(
                0, ConsumerId.DEFAULT, new TestingSubpartitionConsumerInternalOperation());
        assertThatThrownBy(
                        () ->
                                cacheDataManager.registerNewConsumer(
                                        0,
                                        ConsumerId.DEFAULT,
                                        new TestingSubpartitionConsumerInternalOperation()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Each subpartition view should have unique consumerId.");
        cacheDataManager.onConsumerReleased(0, ConsumerId.DEFAULT);
        cacheDataManager.registerNewConsumer(
                0, ConsumerId.DEFAULT, new TestingSubpartitionConsumerInternalOperation());
    }

    private CacheDataManager createCacheDataManager(TsSpillingStrategy spillStrategy)
            throws Exception {
        return createCacheDataManager(
                spillStrategy, new RegionBufferIndexTrackerImpl(NUM_SUBPARTITIONS));
    }

    private CacheDataManager createCacheDataManager(
            TsSpillingStrategy spillStrategy, RegionBufferIndexTracker regionBufferIndexTracker)
            throws Exception {
        NetworkBufferPool networkBufferPool = new NetworkBufferPool(NUM_BUFFERS, bufferSize);
        BufferPool bufferPool = networkBufferPool.createBufferPool(poolSize, poolSize);
        return createCacheDataManager(bufferPool, spillStrategy, regionBufferIndexTracker);
    }

    private CacheDataManager createCacheDataManager(
            TsSpillingStrategy spillingStrategy, BufferPool bufferPool) throws Exception {
        return createCacheDataManager(
                bufferPool, spillingStrategy, new RegionBufferIndexTrackerImpl(NUM_SUBPARTITIONS));
    }

    private CacheDataManager createCacheDataManager(
            TsSpillingStrategy spillingStrategy, BufferPoolHelper bufferPoolHelper)
            throws Exception {
        CacheDataManager cacheDataManager =
                new CacheDataManager(
                        NUM_SUBPARTITIONS,
                        bufferSize,
                        bufferPoolHelper,
                        spillingStrategy,
                        new RegionBufferIndexTrackerImpl(NUM_SUBPARTITIONS),
                        dataFilePath,
                        null);
        cacheDataManager.setOutputMetrics(TieredStoreTestUtils.createTestingOutputMetrics());
        return cacheDataManager;
    }

    private CacheDataManager createCacheDataManager(
            BufferPool bufferPool,
            TsSpillingStrategy spillStrategy,
            RegionBufferIndexTracker regionBufferIndexTracker)
            throws Exception {
        CacheDataManager cacheDataManager =
                new CacheDataManager(
                        NUM_SUBPARTITIONS,
                        bufferSize,
                        new BufferPoolHelperImpl(bufferPool, 0.4f, 0.2f, 0.8f),
                        spillStrategy,
                        regionBufferIndexTracker,
                        dataFilePath,
                        null);
        cacheDataManager.setOutputMetrics(createTestingOutputMetrics());
        return cacheDataManager;
    }

    private static ByteBuffer createRecord(int value) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES);
        byteBuffer.putInt(value);
        byteBuffer.flip();
        return byteBuffer;
    }
}
