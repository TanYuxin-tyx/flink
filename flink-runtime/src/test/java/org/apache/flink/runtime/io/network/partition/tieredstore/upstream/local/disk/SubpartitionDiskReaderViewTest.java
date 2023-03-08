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
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreMemoryManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.UpstreamTieredStoreMemoryManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TestingTierReader;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.DiskCacheManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.RegionBufferIndexTrackerImpl;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.SubpartitionDiskReaderView;

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

/** Tests for {@link SubpartitionDiskReaderView}. */
class SubpartitionDiskReaderViewTest {

    @Test
    void testGetNextBufferFromDisk() throws IOException {
        SubpartitionDiskReaderView subpartitionDiskReaderView = createSubpartitionDiskReaderView();
        BufferAndBacklog bufferAndBacklog = createBufferAndBacklog(1, DataType.DATA_BUFFER, 0);
        TestingTierReader diskTierReader =
                TestingTierReader.builder()
                        .setConsumeBufferFunction(
                                (bufferToConsume) -> Optional.of(bufferAndBacklog))
                        .build();
        subpartitionDiskReaderView.setDiskTierReader(diskTierReader);
        BufferAndBacklog nextBuffer = subpartitionDiskReaderView.getNextBuffer();
        assertThat(nextBuffer).isSameAs(bufferAndBacklog);
    }

    @Test
    void testDeadLock(@TempDir Path dataFilePath) throws Exception {
        final int bufferSize = 16;
        NetworkBufferPool networkBufferPool = new NetworkBufferPool(10, bufferSize);
        BufferPool bufferPool = networkBufferPool.createBufferPool(10, 10);
        TieredStoreMemoryManager tieredStoreMemoryManager =
                new UpstreamTieredStoreMemoryManager(bufferPool, getTierExclusiveBuffers(), 1);
        SubpartitionDiskReaderView subpartitionDiskReaderView = createSubpartitionDiskReaderView();
        CompletableFuture<Void> acquireWriteLock = new CompletableFuture<>();
        CheckedThread consumerThread =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        // blocking until other thread acquire write lock.
                        acquireWriteLock.get();
                        subpartitionDiskReaderView.getNextBuffer();
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
        subpartitionDiskReaderView.setDiskTierReader(TestingTierReader.NO_OP);
        consumerThread.start();
        // trigger request buffer.
        diskCacheManager.append(ByteBuffer.allocate(bufferSize), 0, DataType.DATA_BUFFER, false);
    }

    @Test
    void testGetNextBufferFromDiskNextDataTypeIsNone() throws IOException {
        SubpartitionDiskReaderView subpartitionDiskReaderView = createSubpartitionDiskReaderView();
        BufferAndBacklog bufferAndBacklog = createBufferAndBacklog(0, DataType.NONE, 0);

        TestingTierReader diskTierReader =
                TestingTierReader.builder()
                        .setConsumeBufferFunction(
                                (bufferToConsume) -> Optional.of(bufferAndBacklog))
                        .build();
        subpartitionDiskReaderView.setDiskTierReader(diskTierReader);
        BufferAndBacklog nextBuffer = subpartitionDiskReaderView.getNextBuffer();
        assertThat(nextBuffer).isNotNull();
        assertThat(nextBuffer.buffer()).isSameAs(bufferAndBacklog.buffer());
        assertThat(nextBuffer.buffersInBacklog()).isEqualTo(bufferAndBacklog.buffersInBacklog());
        assertThat(nextBuffer.getSequenceNumber()).isEqualTo(bufferAndBacklog.getSequenceNumber());
        assertThat(nextBuffer.getNextDataType()).isEqualTo(DataType.NONE);
    }

    @Test
    void testGetNextBufferThrowException() {
        SubpartitionDiskReaderView subpartitionDiskReaderView = createSubpartitionDiskReaderView();
        TestingTierReader diskTierReader =
                TestingTierReader.builder()
                        .setConsumeBufferFunction(
                                (nextToConsume) -> {
                                    throw new RuntimeException("expected exception.");
                                })
                        .build();
        subpartitionDiskReaderView.setDiskTierReader(diskTierReader);
        assertThatThrownBy(subpartitionDiskReaderView::getNextBuffer)
                .hasStackTraceContaining("expected exception.");
    }

    @Test
    void testGetNextBufferZeroBacklog() throws IOException {
        SubpartitionDiskReaderView subpartitionDiskReaderView = createSubpartitionDiskReaderView();
        final int diskBacklog = 0;
        BufferAndBacklog targetBufferAndBacklog =
                createBufferAndBacklog(diskBacklog, DataType.DATA_BUFFER, 0);

        TestingTierReader diskTierReader =
                TestingTierReader.builder()
                        .setConsumeBufferFunction(
                                (bufferToConsume) -> Optional.of(targetBufferAndBacklog))
                        .build();
        subpartitionDiskReaderView.setDiskTierReader(diskTierReader);
        assertThat(subpartitionDiskReaderView.getNextBuffer())
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
        SubpartitionDiskReaderView subpartitionDiskReaderView =
                createSubpartitionDiskReaderView(() -> notifyAvailableFuture.complete(null));
        subpartitionDiskReaderView.setDiskTierReader(TestingTierReader.NO_OP);
        subpartitionDiskReaderView.getNextBuffer();
        subpartitionDiskReaderView.notifyDataAvailable();
        assertThat(notifyAvailableFuture).isCompleted();
    }

    @Test
    void testNotifyDataAvailableNotNeedNotify() throws IOException {
        CompletableFuture<Void> notifyAvailableFuture = new CompletableFuture<>();
        SubpartitionDiskReaderView subpartitionDiskReaderView =
                createSubpartitionDiskReaderView(() -> notifyAvailableFuture.complete(null));
        subpartitionDiskReaderView.setDiskTierReader(TestingTierReader.NO_OP);
        subpartitionDiskReaderView.getNextBuffer();
        subpartitionDiskReaderView.notifyDataAvailable();
        assertThat(notifyAvailableFuture).isCompleted();
    }

    @Test
    void testGetZeroBacklogNeedNotify() {
        CompletableFuture<Void> notifyAvailableFuture = new CompletableFuture<>();
        SubpartitionDiskReaderView subpartitionDiskReaderView =
                createSubpartitionDiskReaderView(() -> notifyAvailableFuture.complete(null));
        subpartitionDiskReaderView.setDiskTierReader(
                TestingTierReader.builder().setGetBacklogSupplier(() -> 0).build());
        AvailabilityWithBacklog availabilityAndBacklog =
                subpartitionDiskReaderView.getAvailabilityAndBacklog(0);
        assertThat(availabilityAndBacklog.getBacklog()).isZero();
        assertThat(notifyAvailableFuture).isNotCompleted();
        subpartitionDiskReaderView.notifyDataAvailable();
        assertThat(notifyAvailableFuture).isCompleted();
    }

    @Test
    void testGetAvailabilityAndBacklogPositiveCredit() {
        SubpartitionDiskReaderView subpartitionDiskReaderView = createSubpartitionDiskReaderView();
        final int backlog = 2;
        subpartitionDiskReaderView.setDiskTierReader(
                TestingTierReader.builder().setGetBacklogSupplier(() -> backlog).build());
        AvailabilityWithBacklog availabilityAndBacklog =
                subpartitionDiskReaderView.getAvailabilityAndBacklog(1);
        assertThat(availabilityAndBacklog.getBacklog()).isEqualTo(backlog);
        // positive credit always available.
        assertThat(availabilityAndBacklog.isAvailable()).isTrue();
    }

    @Test
    void testGetAvailabilityAndBacklogNonPositiveCreditNextIsData() throws IOException {
        final int backlog = 2;
        SubpartitionDiskReaderView subpartitionDiskReaderView = createSubpartitionDiskReaderView();
        subpartitionDiskReaderView.setDiskTierReader(
                TestingTierReader.builder().setGetBacklogSupplier(() -> backlog).build());
        subpartitionDiskReaderView.getNextBuffer();
        AvailabilityWithBacklog availabilityAndBacklog =
                subpartitionDiskReaderView.getAvailabilityAndBacklog(0);
        assertThat(availabilityAndBacklog.getBacklog()).isEqualTo(backlog);
        // if credit is non-positive, only event can be available.
        assertThat(availabilityAndBacklog.isAvailable()).isFalse();
    }

    @Test
    void testGetAvailabilityAndBacklogNonPositiveCreditNextIsEvent() throws IOException {
        final int backlog = 2;
        SubpartitionDiskReaderView subpartitionDiskReaderView = createSubpartitionDiskReaderView();
        subpartitionDiskReaderView.setDiskTierReader(
                TestingTierReader.builder().setGetBacklogSupplier(() -> backlog).build());
        subpartitionDiskReaderView.getNextBuffer();
        AvailabilityWithBacklog availabilityAndBacklog =
                subpartitionDiskReaderView.getAvailabilityAndBacklog(0);
        assertThat(availabilityAndBacklog.getBacklog()).isEqualTo(backlog);
        assertThat(availabilityAndBacklog.isAvailable()).isFalse();
    }

    @Test
    void testRelease() throws Exception {
        SubpartitionDiskReaderView subpartitionDiskReaderView = createSubpartitionDiskReaderView();
        CompletableFuture<Void> releaseDiskViewFuture = new CompletableFuture<>();
        TestingTierReader diskTierReader =
                TestingTierReader.builder()
                        .setReleaseDataViewRunnable(() -> releaseDiskViewFuture.complete(null))
                        .build();
        subpartitionDiskReaderView.setDiskTierReader(diskTierReader);
        subpartitionDiskReaderView.releaseAllResources();
        assertThat(subpartitionDiskReaderView.isReleased()).isTrue();
        assertThat(releaseDiskViewFuture).isCompleted();
    }

    @Test
    void testGetConsumingOffset() throws IOException {
        AtomicInteger nextBufferIndex = new AtomicInteger(0);
        SubpartitionDiskReaderView subpartitionDiskReaderView = createSubpartitionDiskReaderView();
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
        subpartitionDiskReaderView.setDiskTierReader(diskTierReader);
        assertThat(subpartitionDiskReaderView.getConsumingOffset(true)).isEqualTo(-1);
        subpartitionDiskReaderView.getNextBuffer();
        assertThat(subpartitionDiskReaderView.getConsumingOffset(true)).isEqualTo(0);
        subpartitionDiskReaderView.getNextBuffer();
        assertThat(subpartitionDiskReaderView.getConsumingOffset(true)).isEqualTo(1);
    }

    @Test
    void testSetDataViewRepeatedly() {
        SubpartitionDiskReaderView subpartitionDiskReaderView = createSubpartitionDiskReaderView();
        subpartitionDiskReaderView.setDiskTierReader(TestingTierReader.NO_OP);
        assertThatThrownBy(() -> subpartitionDiskReaderView.setDiskTierReader(TestingTierReader.NO_OP))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("repeatedly set disk data view is not allowed.");
    }

    private static SubpartitionDiskReaderView createSubpartitionDiskReaderView() {
        return new SubpartitionDiskReaderView(new NoOpBufferAvailablityListener());
    }

    private static SubpartitionDiskReaderView createSubpartitionDiskReaderView(
            BufferAvailabilityListener bufferAvailabilityListener) {
        return new SubpartitionDiskReaderView(bufferAvailabilityListener);
    }

    private static BufferAndBacklog createBufferAndBacklog(
            int buffersInBacklog, DataType nextDataType, int sequenceNumber) {
        final int bufferSize = 8;
        Buffer buffer = TieredStoreTestUtils.createBuffer(bufferSize, true);
        return new BufferAndBacklog(buffer, buffersInBacklog, nextDataType, sequenceNumber, false);
    }
}
