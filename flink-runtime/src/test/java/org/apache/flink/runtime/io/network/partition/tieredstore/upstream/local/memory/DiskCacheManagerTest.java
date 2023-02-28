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

package org.apache.flink.runtime.io.network.partition.tieredstore.upstream.local.memory;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.TieredStoreTestUtils;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.BufferIndexAndChannel;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.BufferPoolHelper;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.BufferPoolHelperImpl;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierReaderViewId;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.DiskCacheManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.RegionBufferIndexTracker;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.RegionBufferIndexTrackerImpl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.runtime.io.network.partition.tieredstore.upstream.TieredStoreTestUtils.createTestingOutputMetrics;
import static org.apache.flink.runtime.io.network.partition.tieredstore.upstream.TieredStoreTestUtils.getTierExclusiveBuffers;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DiskCacheManager}. */
class DiskCacheManagerTest {
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
        bufferSize = Integer.BYTES * 3;
        NetworkBufferPool networkBufferPool = new NetworkBufferPool(NUM_BUFFERS, bufferSize);
        BufferPool bufferPool = networkBufferPool.createBufferPool(poolSize, poolSize);
        BufferPoolHelper bufferPoolHelper =
                new BufferPoolHelperImpl(bufferPool, getTierExclusiveBuffers(), NUM_SUBPARTITIONS);
        DiskCacheManager diskCacheManager = createCacheDataManager(bufferPoolHelper);

        diskCacheManager.append(createRecord(0), 0, Buffer.DataType.DATA_BUFFER, false);
        diskCacheManager.append(createRecord(1), 0, Buffer.DataType.DATA_BUFFER, false);
        diskCacheManager.append(createRecord(2), 0, Buffer.DataType.DATA_BUFFER, false);
        assertThat(finishedBuffers).hasValue(1);

        diskCacheManager.append(createRecord(3), 0, Buffer.DataType.DATA_BUFFER, false);
        assertThat(finishedBuffers).hasValue(1);
        diskCacheManager.append(createRecord(4), 0, Buffer.DataType.DATA_BUFFER, true);
        assertThat(finishedBuffers).hasValue(2);
        diskCacheManager.append(createRecord(5), 0, Buffer.DataType.EVENT_BUFFER, true);
        assertThat(finishedBuffers).hasValue(3);
        diskCacheManager.append(createRecord(6), 0, Buffer.DataType.EVENT_BUFFER, true);
        assertThat(finishedBuffers).hasValue(4);
        diskCacheManager.append(createRecord(7), 0, Buffer.DataType.DATA_BUFFER, true);
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
        DiskCacheManager diskCacheManager = createCacheDataManager(dataIndex);
        for (int i = 0; i < 4; i++) {
            diskCacheManager.append(
                    createRecord(i), targetSubpartition, Buffer.DataType.DATA_BUFFER, false);
        }

        assertThat(spilledFuture).succeedsWithin(10, TimeUnit.SECONDS);
        assertThat(readableFuture).succeedsWithin(10, TimeUnit.SECONDS);
        assertThat(readableFuture).isCompletedWithValue(2);
    }

    @Test
    void testResultPartitionClosed() throws Exception {
        CompletableFuture<Void> resultPartitionReleaseFuture = new CompletableFuture<>();
        DiskCacheManager diskCacheManager = createCacheDataManager();
        diskCacheManager.close();
        assertThat(resultPartitionReleaseFuture).isCompleted();
    }

    @Test
    void testSubpartitionConsumerRelease() throws Exception {
        DiskCacheManager diskCacheManager = createCacheDataManager();
        diskCacheManager.registerNewConsumer(
                0, TierReaderViewId.DEFAULT, new TestingSubpartitionDiskReaderViewOperation());
        assertThatThrownBy(
                        () ->
                                diskCacheManager.registerNewConsumer(
                                        0,
                                        TierReaderViewId.DEFAULT,
                                        new TestingSubpartitionDiskReaderViewOperation()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Each subpartition view should have unique consumerId.");
        diskCacheManager.onConsumerReleased(0, TierReaderViewId.DEFAULT);
        diskCacheManager.registerNewConsumer(
                0, TierReaderViewId.DEFAULT, new TestingSubpartitionDiskReaderViewOperation());
    }

    private DiskCacheManager createCacheDataManager() throws Exception {
        return createCacheDataManager(new RegionBufferIndexTrackerImpl(NUM_SUBPARTITIONS));
    }

    private DiskCacheManager createCacheDataManager(
            RegionBufferIndexTracker regionBufferIndexTracker) throws Exception {
        NetworkBufferPool networkBufferPool = new NetworkBufferPool(NUM_BUFFERS, bufferSize);
        BufferPool bufferPool = networkBufferPool.createBufferPool(poolSize, poolSize);
        return createCacheDataManager(bufferPool, regionBufferIndexTracker);
    }

    private DiskCacheManager createCacheDataManager(BufferPool bufferPool) throws Exception {
        return createCacheDataManager(
                bufferPool, new RegionBufferIndexTrackerImpl(NUM_SUBPARTITIONS));
    }

    private DiskCacheManager createCacheDataManager(BufferPoolHelper bufferPoolHelper)
            throws Exception {
        DiskCacheManager diskCacheManager =
                new DiskCacheManager(
                        NUM_SUBPARTITIONS,
                        bufferSize,
                        bufferPoolHelper,
                        new RegionBufferIndexTrackerImpl(NUM_SUBPARTITIONS),
                        dataFilePath,
                        null);
        diskCacheManager.setOutputMetrics(TieredStoreTestUtils.createTestingOutputMetrics());
        return diskCacheManager;
    }

    private DiskCacheManager createCacheDataManager(
            BufferPool bufferPool, RegionBufferIndexTracker regionBufferIndexTracker)
            throws Exception {
        DiskCacheManager diskCacheManager =
                new DiskCacheManager(
                        NUM_SUBPARTITIONS,
                        bufferSize,
                        new BufferPoolHelperImpl(
                                bufferPool, getTierExclusiveBuffers(), NUM_SUBPARTITIONS),
                        regionBufferIndexTracker,
                        dataFilePath,
                        null);
        diskCacheManager.setOutputMetrics(createTestingOutputMetrics());
        return diskCacheManager;
    }

    private static ByteBuffer createRecord(int value) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES);
        byteBuffer.putInt(value);
        byteBuffer.flip();
        return byteBuffer;
    }
}
