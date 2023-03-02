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

import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.Buffer.DataType;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.NoOpBufferAvailablityListener;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView.AvailabilityWithBacklog;
import org.apache.flink.runtime.io.network.partition.tieredstore.TestingTierReader;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.TieredStoreTestUtils;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierReader;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierReaderViewId;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreMemoryManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.UpstreamTieredStoreMemoryManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.DiskCacheManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.RegionBufferIndexTrackerImpl;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.SubpartitionDiskReaderView;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
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
class LocalSubpartitionDiskReaderTest {
    @Test
    void testGetNextBufferFromDisk() throws IOException {
        SubpartitionDiskReaderView subpartitionView = createSubpartitionView();

        BufferAndBacklog bufferAndBacklog = createBufferAndBacklog(1, DataType.DATA_BUFFER, 0);
        CompletableFuture<Void> consumeBufferFromMemoryFuture = new CompletableFuture<>();
        TestingTierReader diskDataView =
                TestingTierReader.builder()
                        .setConsumeBufferFunction(
                                (bufferToConsume) -> Optional.of(bufferAndBacklog))
                        .build();
        TestingTierReader memoryDataView =
                TestingTierReader.builder()
                        .setConsumeBufferFunction(
                                (ignore) -> {
                                    consumeBufferFromMemoryFuture.complete(null);
                                    return Optional.empty();
                                })
                        .build();
        subpartitionView.setDiskReader(diskDataView);
        // subpartitionView.setMemoryDataView(memoryDataView);

        BufferAndBacklog nextBuffer = subpartitionView.getNextBuffer();
        assertThat(consumeBufferFromMemoryFuture).isNotCompleted();
        assertThat(nextBuffer).isSameAs(bufferAndBacklog);
    }

    @Test
    @Timeout(60)
    void testDeadLock(@TempDir Path dataFilePath) throws Exception {
        final int bufferSize = 16;
        NetworkBufferPool networkBufferPool = new NetworkBufferPool(10, bufferSize);
        BufferPool bufferPool = networkBufferPool.createBufferPool(10, 10);
        TieredStoreMemoryManager tieredStoreMemoryManager =
                new UpstreamTieredStoreMemoryManager(bufferPool, getTierExclusiveBuffers(), 1);
        SubpartitionDiskReaderView subpartitionView = createSubpartitionView();

        CompletableFuture<Void> acquireWriteLock = new CompletableFuture<>();

        CheckedThread consumerThread =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        // blocking until other thread acquire write lock.
                        acquireWriteLock.get();
                        subpartitionView.getNextBuffer();
                    }
                };

        DiskCacheManager diskCacheManager =
                new DiskCacheManager(
                        1,
                        bufferSize,
                        tieredStoreMemoryManager,
                        new RegionBufferIndexTrackerImpl(1),
                        dataFilePath.resolve(".data"),
                        null);
        diskCacheManager.setOutputMetrics(createTestingOutputMetrics());
        TierReader tierReader =
                diskCacheManager.registerNewConsumer(0, TierReaderViewId.DEFAULT, subpartitionView);
        // subpartitionView.setMemoryDataView(bufferConsumeView);
        subpartitionView.setDiskReader(TestingTierReader.NO_OP);

        consumerThread.start();
        // trigger request buffer.
        diskCacheManager.append(ByteBuffer.allocate(bufferSize), 0, DataType.DATA_BUFFER, false);
    }

    // @Test
    // void testGetNextBufferFromDiskNextDataTypeIsNone() throws IOException {
    //    SubpartitionConsumer subpartitionView = createSubpartitionView();
    //    BufferAndBacklog bufferAndBacklog = createBufferAndBacklog(0, DataType.NONE, 0);
    //
    //    TestingBufferConsumeView diskDataView =
    //            TestingBufferConsumeView.builder()
    //                    .setConsumeBufferFunction(
    //                            (bufferToConsume) -> Optional.of(bufferAndBacklog))
    //                    .build();
    //
    //    TestingBufferConsumeView memoryDataView =
    //            TestingBufferConsumeView.builder()
    //                    .setPeekNextToConsumeDataTypeFunction(
    //                            (bufferToConsume) -> {
    //                                assertThat(bufferToConsume).isEqualTo(1);
    //                                return DataType.EVENT_BUFFER;
    //                            })
    //                    .build();
    //    subpartitionView.setDiskDataView(diskDataView);
    //    //subpartitionView.setMemoryDataView(memoryDataView);
    //
    //    BufferAndBacklog nextBuffer = subpartitionView.getNextBuffer();
    //    assertThat(nextBuffer).isNotNull();
    //    assertThat(nextBuffer.buffer()).isSameAs(bufferAndBacklog.buffer());
    //    assertThat(nextBuffer.buffersInBacklog()).isEqualTo(bufferAndBacklog.buffersInBacklog());
    //
    // assertThat(nextBuffer.getSequenceNumber()).isEqualTo(bufferAndBacklog.getSequenceNumber());
    //    assertThat(nextBuffer.getNextDataType()).isEqualTo(DataType.EVENT_BUFFER);
    // }

    // @Test
    // void testGetNextBufferFromMemory() throws IOException {
    //    SubpartitionConsumer subpartitionView = createSubpartitionView();
    //
    //    BufferAndBacklog bufferAndBacklog = createBufferAndBacklog(1, DataType.DATA_BUFFER, 0);
    //    TestingBufferConsumeView memoryDataView =
    //            TestingBufferConsumeView.builder()
    //                    .setConsumeBufferFunction(
    //                            (bufferToConsume) -> Optional.of(bufferAndBacklog))
    //                    .build();
    //    TestingBufferConsumeView diskDataView =
    //            TestingBufferConsumeView.builder()
    //                    .setConsumeBufferFunction((bufferToConsume) -> Optional.empty())
    //                    .build();
    //    subpartitionView.setDiskDataView(diskDataView);
    //    //subpartitionView.setMemoryDataView(memoryDataView);
    //
    //    BufferAndBacklog nextBuffer = subpartitionView.getNextBuffer();
    //    assertThat(nextBuffer).isSameAs(bufferAndBacklog);
    // }

    @Test
    void testGetNextBufferThrowException() {
        SubpartitionDiskReaderView subpartitionView = createSubpartitionView();

        TestingTierReader diskDataView =
                TestingTierReader.builder()
                        .setConsumeBufferFunction(
                                (nextToConsume) -> {
                                    throw new RuntimeException("expected exception.");
                                })
                        .build();
        subpartitionView.setDiskReader(diskDataView);
        // subpartitionView.setMemoryDataView(TestingBufferConsumeView.NO_OP);

        assertThatThrownBy(subpartitionView::getNextBuffer)
                .hasStackTraceContaining("expected exception.");
    }

    // @Test
    // void testGetNextBufferZeroBacklog() throws IOException {
    //    SubpartitionConsumer subpartitionView = createSubpartitionView();
    //
    //    final int diskBacklog = 0;
    //    final int memoryBacklog = 10;
    //    BufferAndBacklog targetBufferAndBacklog =
    //            createBufferAndBacklog(diskBacklog, DataType.DATA_BUFFER, 0);
    //
    //    TestingBufferConsumeView diskDataView =
    //            TestingBufferConsumeView.builder()
    //                    .setConsumeBufferFunction(
    //                            (bufferToConsume) -> Optional.of(targetBufferAndBacklog))
    //                    .build();
    //    TestingBufferConsumeView memoryDataView =
    //            TestingBufferConsumeView.builder()
    //                    .setGetBacklogSupplier(() -> memoryBacklog)
    //                    .build();
    //    subpartitionView.setDiskDataView(diskDataView);
    //    //subpartitionView.setMemoryDataView(memoryDataView);
    //
    //    assertThat(subpartitionView.getNextBuffer())
    //            .satisfies(
    //                    (bufferAndBacklog -> {
    //                        // backlog is reset to maximum backlog of memory and disk.
    //                        assertThat(bufferAndBacklog.buffersInBacklog())
    //                                .isEqualTo(memoryBacklog);
    //                        // other field is not changed.
    //                        assertThat(bufferAndBacklog.buffer())
    //                                .isEqualTo(targetBufferAndBacklog.buffer());
    //                        assertThat(bufferAndBacklog.getNextDataType())
    //                                .isEqualTo(targetBufferAndBacklog.getNextDataType());
    //                        assertThat(bufferAndBacklog.getSequenceNumber())
    //                                .isEqualTo(targetBufferAndBacklog.getSequenceNumber());
    //                    }));
    // }

    @Test
    void testNotifyDataAvailableNeedNotify() throws IOException {
        CompletableFuture<Void> notifyAvailableFuture = new CompletableFuture<>();
        SubpartitionDiskReaderView subpartitionView =
                createSubpartitionView(() -> notifyAvailableFuture.complete(null));

        TestingTierReader memoryDataView =
                TestingTierReader.builder()
                        .setConsumeBufferFunction(
                                (bufferToConsume) ->
                                        Optional.of(createBufferAndBacklog(0, DataType.NONE, 0)))
                        .build();
        // subpartitionView.setMemoryDataView(memoryDataView);
        subpartitionView.setDiskReader(TestingTierReader.NO_OP);

        subpartitionView.getNextBuffer();
        subpartitionView.notifyDataAvailable();
        assertThat(notifyAvailableFuture).isCompleted();
    }

    // @Test
    // void testNotifyDataAvailableNotNeedNotify() throws IOException {
    //    CompletableFuture<Void> notifyAvailableFuture = new CompletableFuture<>();
    //    SubpartitionConsumer subpartitionView =
    //            createSubpartitionView(() -> notifyAvailableFuture.complete(null));
    //
    //    TestingBufferConsumeView memoryDataView =
    //            TestingBufferConsumeView.builder()
    //                    .setConsumeBufferFunction(
    //                            (bufferToConsume) ->
    //                                    Optional.of(
    //                                            createBufferAndBacklog(0, DataType.DATA_BUFFER,
    // 0)))
    //                    .build();
    //    //subpartitionView.setMemoryDataView(memoryDataView);
    //    subpartitionView.setDiskDataView(TestingBufferConsumeView.NO_OP);
    //
    //    subpartitionView.getNextBuffer();
    //    subpartitionView.notifyDataAvailable();
    //    assertThat(notifyAvailableFuture).isNotCompleted();
    // }

    @Test
    void testGetZeroBacklogNeedNotify() {
        CompletableFuture<Void> notifyAvailableFuture = new CompletableFuture<>();
        SubpartitionDiskReaderView subpartitionView =
                createSubpartitionView(() -> notifyAvailableFuture.complete(null));
        // subpartitionView.setMemoryDataView(TestingBufferConsumeView.NO_OP);
        subpartitionView.setDiskReader(
                TestingTierReader.builder().setGetBacklogSupplier(() -> 0).build());

        AvailabilityWithBacklog availabilityAndBacklog =
                subpartitionView.getAvailabilityAndBacklog(0);
        assertThat(availabilityAndBacklog.getBacklog()).isZero();

        assertThat(notifyAvailableFuture).isNotCompleted();
        subpartitionView.notifyDataAvailable();
        assertThat(notifyAvailableFuture).isCompleted();
    }

    @Test
    void testGetAvailabilityAndBacklogPositiveCredit() {
        SubpartitionDiskReaderView subpartitionView = createSubpartitionView();
        // subpartitionView.setMemoryDataView(TestingBufferConsumeView.NO_OP);

        final int backlog = 2;
        subpartitionView.setDiskReader(
                TestingTierReader.builder().setGetBacklogSupplier(() -> backlog).build());
        AvailabilityWithBacklog availabilityAndBacklog =
                subpartitionView.getAvailabilityAndBacklog(1);
        assertThat(availabilityAndBacklog.getBacklog()).isEqualTo(backlog);
        // positive credit always available.
        assertThat(availabilityAndBacklog.isAvailable()).isTrue();
    }

    @Test
    void testGetAvailabilityAndBacklogNonPositiveCreditNextIsData() throws IOException {
        final int backlog = 2;

        SubpartitionDiskReaderView subpartitionView = createSubpartitionView();
        // subpartitionView.setMemoryDataView(
        //        TestingBufferConsumeView.builder()
        //                .setConsumeBufferFunction(
        //                        (nextToConsume) ->
        //                                Optional.of(
        //                                        createBufferAndBacklog(
        //                                                backlog, DataType.DATA_BUFFER, 0)))
        //                .build());
        subpartitionView.setDiskReader(
                TestingTierReader.builder().setGetBacklogSupplier(() -> backlog).build());

        subpartitionView.getNextBuffer();

        AvailabilityWithBacklog availabilityAndBacklog =
                subpartitionView.getAvailabilityAndBacklog(0);
        assertThat(availabilityAndBacklog.getBacklog()).isEqualTo(backlog);
        // if credit is non-positive, only event can be available.
        assertThat(availabilityAndBacklog.isAvailable()).isFalse();
    }

    // @Test
    // void testGetAvailabilityAndBacklogNonPositiveCreditNextIsEvent() throws IOException {
    //    final int backlog = 2;
    //
    //    SubpartitionConsumer subpartitionView = createSubpartitionView();
    //    //subpartitionView.setMemoryDataView(
    //    //        TestingBufferConsumeView.builder()
    //    //                .setConsumeBufferFunction(
    //    //                        (nextToConsume) ->
    //    //                                Optional.of(
    //    //                                        createBufferAndBacklog(
    //    //                                                backlog, DataType.EVENT_BUFFER, 0)))
    //    //                .build());
    //    subpartitionView.setDiskDataView(
    //            TestingBufferConsumeView.builder().setGetBacklogSupplier(() -> backlog).build());
    //
    //    subpartitionView.getNextBuffer();
    //
    //    AvailabilityWithBacklog availabilityAndBacklog =
    //            subpartitionView.getAvailabilityAndBacklog(0);
    //    assertThat(availabilityAndBacklog.getBacklog()).isEqualTo(backlog);
    //    // if credit is non-positive, only event can be available.
    //    assertThat(availabilityAndBacklog.isAvailable()).isTrue();
    // }

    // @Test
    // void testRelease() throws Exception {
    //    SubpartitionConsumer subpartitionView = createSubpartitionView();
    //    CompletableFuture<Void> releaseDiskViewFuture = new CompletableFuture<>();
    //    CompletableFuture<Void> releaseMemoryViewFuture = new CompletableFuture<>();
    //    TestingBufferConsumeView diskDataView =
    //            TestingBufferConsumeView.builder()
    //                    .setReleaseDataViewRunnable(() -> releaseDiskViewFuture.complete(null))
    //                    .build();
    //    TestingBufferConsumeView memoryDataView =
    //            TestingBufferConsumeView.builder()
    //                    .setReleaseDataViewRunnable(() -> releaseMemoryViewFuture.complete(null))
    //                    .build();
    //    subpartitionView.setDiskDataView(diskDataView);
    //    //subpartitionView.setMemoryDataView(memoryDataView);
    //    subpartitionView.releaseAllResources();
    //    assertThat(subpartitionView.isReleased()).isTrue();
    //    assertThat(releaseDiskViewFuture).isCompleted();
    //    assertThat(releaseMemoryViewFuture).isCompleted();
    // }

    @Test
    void testGetConsumingOffset() throws IOException {
        AtomicInteger nextBufferIndex = new AtomicInteger(0);
        SubpartitionDiskReaderView subpartitionView = createSubpartitionView();
        TestingTierReader diskDataView =
                TestingTierReader.builder()
                        .setConsumeBufferFunction(
                                (toConsumeBuffer) ->
                                        Optional.of(
                                                createBufferAndBacklog(
                                                        0,
                                                        DataType.DATA_BUFFER,
                                                        nextBufferIndex.getAndIncrement())))
                        .build();
        subpartitionView.setDiskReader(diskDataView);
        // subpartitionView.setMemoryDataView(TestingBufferConsumeView.NO_OP);

        assertThat(subpartitionView.getConsumingOffset(true)).isEqualTo(-1);
        subpartitionView.getNextBuffer();
        assertThat(subpartitionView.getConsumingOffset(true)).isEqualTo(0);
        subpartitionView.getNextBuffer();
        assertThat(subpartitionView.getConsumingOffset(true)).isEqualTo(1);
    }

    @Test
    void testSetDataViewRepeatedly() {
        SubpartitionDiskReaderView subpartitionView = createSubpartitionView();

        // subpartitionView.setMemoryDataView(TestingBufferConsumeView.NO_OP);
        // assertThatThrownBy(() ->
        // subpartitionView.setMemoryDataView(TestingBufferConsumeView.NO_OP))
        //        .isInstanceOf(IllegalStateException.class)
        //        .hasMessageContaining("repeatedly set memory data view is not allowed.");

        subpartitionView.setDiskReader(TestingTierReader.NO_OP);
        assertThatThrownBy(() -> subpartitionView.setDiskReader(TestingTierReader.NO_OP))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("repeatedly set disk data view is not allowed.");
    }

    private static SubpartitionDiskReaderView createSubpartitionView() {
        return new SubpartitionDiskReaderView(new NoOpBufferAvailablityListener());
    }

    private static SubpartitionDiskReaderView createSubpartitionView(
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
