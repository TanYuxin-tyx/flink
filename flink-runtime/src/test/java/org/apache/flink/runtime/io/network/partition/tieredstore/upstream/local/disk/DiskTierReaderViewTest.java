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

package org.apache.flink.runtime.io.network.partition.tieredstore.upstream.local.disk;

import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.Buffer.DataType;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.NoOpBufferAvailablityListener;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView.AvailabilityWithBacklog;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.TieredStoreTestUtils;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.CacheFlushManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TestingTierReader;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierReaderView;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierReaderViewImpl;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreMemoryManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.UpstreamTieredStoreMemoryManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.DiskCacheManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.DiskTierReader;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.RegionBufferIndexTrackerImpl;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.runtime.io.network.partition.tieredstore.upstream.TieredStoreTestUtils.createTestingOutputMetrics;
import static org.apache.flink.runtime.io.network.partition.tieredstore.upstream.TieredStoreTestUtils.getTierExclusiveBuffers;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DiskTierReader}. */
class DiskTierReaderViewTest {

    @Test
    void testGetNextBufferFromDisk() throws IOException {
        TierReaderView diskTierReaderView = createTierReaderView();
        BufferAndBacklog bufferAndBacklog = createBufferAndBacklog(1, DataType.DATA_BUFFER, 0);
        TestingTierReader diskTierReader =
                TestingTierReader.builder()
                        .setConsumeBufferFunction(
                                (bufferToConsume) -> Optional.of(bufferAndBacklog))
                        .build();
        diskTierReaderView.setTierReader(diskTierReader);
        BufferAndBacklog nextBuffer = diskTierReaderView.getNextBuffer();
        assertThat(nextBuffer).isSameAs(bufferAndBacklog);
    }

    @Test
    void testDeadLock(@TempDir Path dataFilePath) throws Exception {
        final int bufferSize = 16;
        NetworkBufferPool networkBufferPool = new NetworkBufferPool(10, bufferSize);
        BufferPool bufferPool = networkBufferPool.createBufferPool(10, 10);
        TieredStoreMemoryManager tieredStoreMemoryManager =
                new UpstreamTieredStoreMemoryManager(bufferPool, getTierExclusiveBuffers(), 1);
        TierReaderView diskTierReaderView = createTierReaderView();
        CompletableFuture<Void> acquireWriteLock = new CompletableFuture<>();
        CheckedThread consumerThread =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        // blocking until other thread acquire write lock.
                        acquireWriteLock.get();
                        diskTierReaderView.getNextBuffer();
                    }
                };
        DiskCacheManager diskCacheManager =
                new DiskCacheManager(
                        1,
                        bufferSize,
                        tieredStoreMemoryManager,
                        new CacheFlushManager(),
                        new RegionBufferIndexTrackerImpl(1),
                        dataFilePath.resolve(".data"),
                        null);
        diskCacheManager.setOutputMetrics(createTestingOutputMetrics());
        diskTierReaderView.setTierReader(TestingTierReader.NO_OP);
        consumerThread.start();
        // trigger request buffer.
        diskCacheManager.append(ByteBuffer.allocate(bufferSize), 0, DataType.DATA_BUFFER, false);
    }

    @Test
    void testGetNextBufferFromDiskNextDataTypeIsNone() throws IOException {
        TierReaderView diskTierReaderView = createTierReaderView();
        BufferAndBacklog bufferAndBacklog = createBufferAndBacklog(0, DataType.NONE, 0);

        TestingTierReader diskTierReader =
                TestingTierReader.builder()
                        .setConsumeBufferFunction(
                                (bufferToConsume) -> Optional.of(bufferAndBacklog))
                        .build();
        diskTierReaderView.setTierReader(diskTierReader);
        BufferAndBacklog nextBuffer = diskTierReaderView.getNextBuffer();
        assertThat(nextBuffer).isNotNull();
        assertThat(nextBuffer.buffer()).isSameAs(bufferAndBacklog.buffer());
        assertThat(nextBuffer.buffersInBacklog()).isEqualTo(bufferAndBacklog.buffersInBacklog());
        assertThat(nextBuffer.getSequenceNumber()).isEqualTo(bufferAndBacklog.getSequenceNumber());
        assertThat(nextBuffer.getNextDataType()).isEqualTo(DataType.NONE);
    }

    @Test
    void testGetNextBufferThrowException() {
        TierReaderView diskTierReaderView = createTierReaderView();
        TestingTierReader diskTierReader =
                TestingTierReader.builder()
                        .setConsumeBufferFunction(
                                (nextToConsume) -> {
                                    throw new RuntimeException("expected exception.");
                                })
                        .build();
        diskTierReaderView.setTierReader(diskTierReader);
        assertThatThrownBy(diskTierReaderView::getNextBuffer)
                .hasStackTraceContaining("expected exception.");
    }

    @Test
    void testGetNextBufferZeroBacklog() throws IOException {
        TierReaderView diskTierReaderView = createTierReaderView();
        final int diskBacklog = 0;
        BufferAndBacklog targetBufferAndBacklog =
                createBufferAndBacklog(diskBacklog, DataType.DATA_BUFFER, 0);

        TestingTierReader diskTierReader =
                TestingTierReader.builder()
                        .setConsumeBufferFunction(
                                (bufferToConsume) -> Optional.of(targetBufferAndBacklog))
                        .build();
        diskTierReaderView.setTierReader(diskTierReader);
        assertThat(diskTierReaderView.getNextBuffer())
                .satisfies(
                        (bufferAndBacklog -> {
                            // backlog is reset to maximum backlog of memory and disk.
                            assertThat(bufferAndBacklog.buffersInBacklog()).isEqualTo(diskBacklog);
                            // other field is not changed.
                            assertThat(bufferAndBacklog.buffer())
                                    .isEqualTo(targetBufferAndBacklog.buffer());
                            assertThat(bufferAndBacklog.getNextDataType())
                                    .isEqualTo(targetBufferAndBacklog.getNextDataType());
                            assertThat(bufferAndBacklog.getSequenceNumber())
                                    .isEqualTo(targetBufferAndBacklog.getSequenceNumber());
                        }));
    }

    @Test
    void testNotifyDataAvailableNeedNotify() throws IOException {
        CompletableFuture<Void> notifyAvailableFuture = new CompletableFuture<>();
        TierReaderView diskTierReaderView =
                createTierReaderView(() -> notifyAvailableFuture.complete(null));
        diskTierReaderView.setTierReader(TestingTierReader.NO_OP);
        diskTierReaderView.getNextBuffer();
        diskTierReaderView.notifyDataAvailable();
        assertThat(notifyAvailableFuture).isCompleted();
    }

    @Test
    void testNotifyDataAvailableNotNeedNotify() throws IOException {
        CompletableFuture<Void> notifyAvailableFuture = new CompletableFuture<>();
        TierReaderView diskTierReaderView =
                createTierReaderView(() -> notifyAvailableFuture.complete(null));
        diskTierReaderView.setTierReader(TestingTierReader.NO_OP);
        diskTierReaderView.getNextBuffer();
        diskTierReaderView.notifyDataAvailable();
        assertThat(notifyAvailableFuture).isCompleted();
    }

    @Test
    void testGetZeroBacklogNeedNotify() {
        CompletableFuture<Void> notifyAvailableFuture = new CompletableFuture<>();
        TierReaderView diskTierReaderView =
                createTierReaderView(() -> notifyAvailableFuture.complete(null));
        diskTierReaderView.setTierReader(
                TestingTierReader.builder().setGetBacklogSupplier(() -> 0).build());
        AvailabilityWithBacklog availabilityAndBacklog =
                diskTierReaderView.getAvailabilityAndBacklog(0);
        assertThat(availabilityAndBacklog.getBacklog()).isZero();
        assertThat(notifyAvailableFuture).isNotCompleted();
        diskTierReaderView.notifyDataAvailable();
        assertThat(notifyAvailableFuture).isCompleted();
    }

    @Test
    void testGetAvailabilityAndBacklogPositiveCredit() {
        TierReaderView diskTierReaderView = createTierReaderView();
        final int backlog = 2;
        diskTierReaderView.setTierReader(
                TestingTierReader.builder().setGetBacklogSupplier(() -> backlog).build());
        AvailabilityWithBacklog availabilityAndBacklog =
                diskTierReaderView.getAvailabilityAndBacklog(1);
        assertThat(availabilityAndBacklog.getBacklog()).isEqualTo(backlog);
        // positive credit always available.
        assertThat(availabilityAndBacklog.isAvailable()).isTrue();
    }

    @Test
    void testGetAvailabilityAndBacklogNonPositiveCreditNextIsData() throws IOException {
        final int backlog = 2;
        TierReaderView diskTierReaderView = createTierReaderView();
        diskTierReaderView.setTierReader(
                TestingTierReader.builder().setGetBacklogSupplier(() -> backlog).build());
        diskTierReaderView.getNextBuffer();
        AvailabilityWithBacklog availabilityAndBacklog =
                diskTierReaderView.getAvailabilityAndBacklog(0);
        assertThat(availabilityAndBacklog.getBacklog()).isEqualTo(backlog);
        // if credit is non-positive, only event can be available.
        assertThat(availabilityAndBacklog.isAvailable()).isFalse();
    }

    @Test
    void testGetAvailabilityAndBacklogNonPositiveCreditNextIsEvent() throws IOException {
        final int backlog = 2;
        TierReaderView diskTierReaderView = createTierReaderView();
        diskTierReaderView.setTierReader(
                TestingTierReader.builder().setGetBacklogSupplier(() -> backlog).build());
        diskTierReaderView.getNextBuffer();
        AvailabilityWithBacklog availabilityAndBacklog =
                diskTierReaderView.getAvailabilityAndBacklog(0);
        assertThat(availabilityAndBacklog.getBacklog()).isEqualTo(backlog);
        assertThat(availabilityAndBacklog.isAvailable()).isFalse();
    }

    @Test
    void testRelease() throws Exception {
        TierReaderView diskTierReaderView = createTierReaderView();
        CompletableFuture<Void> releaseDiskViewFuture = new CompletableFuture<>();
        TestingTierReader diskTierReader =
                TestingTierReader.builder()
                        .setReleaseDataViewRunnable(() -> releaseDiskViewFuture.complete(null))
                        .build();
        diskTierReaderView.setTierReader(diskTierReader);
        diskTierReaderView.releaseAllResources();
        assertThat(diskTierReaderView.isReleased()).isTrue();
        assertThat(releaseDiskViewFuture).isCompleted();
    }

    @Test
    void testGetConsumingOffset() throws IOException {
        AtomicInteger nextBufferIndex = new AtomicInteger(0);
        TierReaderView diskTierReaderView = createTierReaderView();
        TestingTierReader diskTierReader =
                TestingTierReader.builder()
                        .setConsumeBufferFunction(
                                (toConsumeBuffer) ->
                                        Optional.of(
                                                createBufferAndBacklog(
                                                        0,
                                                        DataType.DATA_BUFFER,
                                                        nextBufferIndex.getAndIncrement())))
                        .build();
        diskTierReaderView.setTierReader(diskTierReader);
        assertThat(diskTierReaderView.getConsumingOffset(true)).isEqualTo(-1);
        diskTierReaderView.getNextBuffer();
        assertThat(diskTierReaderView.getConsumingOffset(true)).isEqualTo(0);
        diskTierReaderView.getNextBuffer();
        assertThat(diskTierReaderView.getConsumingOffset(true)).isEqualTo(1);
    }

    @Test
    void testSetDataViewRepeatedly() {
        TierReaderView tierReaderView = createTierReaderView();
        tierReaderView.setTierReader(TestingTierReader.NO_OP);
        assertThatThrownBy(() -> tierReaderView.setTierReader(TestingTierReader.NO_OP))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("repeatedly set disk data view is not allowed.");
    }

    private static TierReaderView createTierReaderView() {
        return new TierReaderViewImpl(new NoOpBufferAvailablityListener());
    }

    private static TierReaderView createTierReaderView(
            BufferAvailabilityListener bufferAvailabilityListener) {
        return new TierReaderViewImpl(bufferAvailabilityListener);
    }

    private static BufferAndBacklog createBufferAndBacklog(
            int buffersInBacklog, DataType nextDataType, int sequenceNumber) {
        final int bufferSize = 8;
        Buffer buffer = TieredStoreTestUtils.createBuffer(bufferSize, true);
        return new BufferAndBacklog(buffer, buffersInBacklog, nextDataType, sequenceNumber, false);
    }
}
