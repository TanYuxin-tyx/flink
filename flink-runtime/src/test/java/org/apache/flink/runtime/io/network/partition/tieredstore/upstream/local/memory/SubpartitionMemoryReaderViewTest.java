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
import org.apache.flink.runtime.io.network.buffer.Buffer.DataType;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.NoOpBufferAvailablityListener;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView.AvailabilityWithBacklog;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.TieredStoreTestUtils;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TestingTierReader;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.memory.MemoryTierReaderView;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link MemoryTierReaderView}. */
class SubpartitionMemoryReaderViewTest {

    @Test
    void testGetNextBufferFromMemory() throws IOException {
        MemoryTierReaderView memoryTierReaderView =
                createSubpartitionMemoryReaderView();
        BufferAndBacklog bufferAndBacklog = createBufferAndBacklog(1, DataType.DATA_BUFFER, 0);
        TestingTierReader memoryTierReader =
                TestingTierReader.builder()
                        .setConsumeBufferFunction(
                                (bufferToConsume) -> Optional.of(bufferAndBacklog))
                        .build();
        memoryTierReaderView.setMemoryTierReader(memoryTierReader);
        BufferAndBacklog nextBuffer = memoryTierReaderView.getNextBuffer();
        assertThat(nextBuffer).isSameAs(bufferAndBacklog);
    }

    @Test
    void testGetNextBufferFromMemoryNextDataTypeIsNone() throws IOException {
        MemoryTierReaderView memoryTierReaderView =
                createSubpartitionMemoryReaderView();
        BufferAndBacklog bufferAndBacklog = createBufferAndBacklog(0, DataType.NONE, 0);
        TestingTierReader memoryTierReader =
                TestingTierReader.builder()
                        .setConsumeBufferFunction(
                                (bufferToConsume) -> Optional.of(bufferAndBacklog))
                        .build();
        memoryTierReaderView.setMemoryTierReader(memoryTierReader);
        BufferAndBacklog nextBuffer = memoryTierReaderView.getNextBuffer();
        assertThat(nextBuffer).isNotNull();
        assertThat(nextBuffer.buffer()).isSameAs(bufferAndBacklog.buffer());
        assertThat(nextBuffer.buffersInBacklog()).isEqualTo(bufferAndBacklog.buffersInBacklog());
        assertThat(nextBuffer.getSequenceNumber()).isEqualTo(bufferAndBacklog.getSequenceNumber());
        assertThat(nextBuffer.getNextDataType()).isEqualTo(DataType.NONE);
    }

    @Test
    void testGetNextBufferThrowException() {
        MemoryTierReaderView memoryTierReaderView =
                createSubpartitionMemoryReaderView();
        TestingTierReader memoryTierReader =
                TestingTierReader.builder()
                        .setConsumeBufferFunction(
                                (nextToConsume) -> {
                                    throw new RuntimeException("expected exception.");
                                })
                        .build();
        memoryTierReaderView.setMemoryTierReader(memoryTierReader);
        assertThatThrownBy(memoryTierReaderView::getNextBuffer)
                .hasStackTraceContaining("expected exception.");
    }

    @Test
    void testGetNextBufferZeroBacklog() throws IOException {
        MemoryTierReaderView memoryTierReaderView =
                createSubpartitionMemoryReaderView();
        int diskBacklog = 0;
        BufferAndBacklog targetBufferAndBacklog =
                createBufferAndBacklog(diskBacklog, DataType.DATA_BUFFER, 0);
        TestingTierReader memoryTierReader =
                TestingTierReader.builder()
                        .setConsumeBufferFunction(
                                (bufferToConsume) -> Optional.of(targetBufferAndBacklog))
                        .build();
        memoryTierReaderView.setMemoryTierReader(memoryTierReader);
        assertThat(memoryTierReaderView.getNextBuffer())
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
        MemoryTierReaderView memoryTierReaderView =
                createSubpartitionMemoryReaderView(() -> notifyAvailableFuture.complete(null));
        memoryTierReaderView.setMemoryTierReader(TestingTierReader.NO_OP);
        memoryTierReaderView.getNextBuffer();
        memoryTierReaderView.notifyDataAvailable();
        assertThat(notifyAvailableFuture).isCompleted();
    }

    @Test
    void testNotifyDataAvailableNotNeedNotify() throws IOException {
        CompletableFuture<Void> notifyAvailableFuture = new CompletableFuture<>();
        MemoryTierReaderView memoryTierReaderView =
                createSubpartitionMemoryReaderView(() -> notifyAvailableFuture.complete(null));
        memoryTierReaderView.setMemoryTierReader(TestingTierReader.NO_OP);
        memoryTierReaderView.getNextBuffer();
        memoryTierReaderView.notifyDataAvailable();
        assertThat(notifyAvailableFuture).isCompleted();
    }

    @Test
    void testGetZeroBacklogNeedNotify() {
        CompletableFuture<Void> notifyAvailableFuture = new CompletableFuture<>();
        MemoryTierReaderView memoryTierReaderView =
                createSubpartitionMemoryReaderView(() -> notifyAvailableFuture.complete(null));
        memoryTierReaderView.setMemoryTierReader(
                TestingTierReader.builder().setGetBacklogSupplier(() -> 0).build());
        AvailabilityWithBacklog availabilityAndBacklog =
                memoryTierReaderView.getAvailabilityAndBacklog(0);
        assertThat(availabilityAndBacklog.getBacklog()).isZero();
        assertThat(notifyAvailableFuture).isNotCompleted();
        memoryTierReaderView.notifyDataAvailable();
        assertThat(notifyAvailableFuture).isCompleted();
    }

    @Test
    void testGetAvailabilityAndBacklogPositiveCredit() {
        MemoryTierReaderView memoryTierReaderView =
                createSubpartitionMemoryReaderView();
        int backlog = 2;
        memoryTierReaderView.setMemoryTierReader(
                TestingTierReader.builder().setGetBacklogSupplier(() -> backlog).build());
        AvailabilityWithBacklog availabilityAndBacklog =
                memoryTierReaderView.getAvailabilityAndBacklog(1);
        assertThat(availabilityAndBacklog.getBacklog()).isEqualTo(backlog);
        // positive credit always available.
        assertThat(availabilityAndBacklog.isAvailable()).isTrue();
    }

    @Test
    void testGetAvailabilityAndBacklogNonPositiveCreditNextIsData() throws IOException {
        int backlog = 2;
        MemoryTierReaderView memoryTierReaderView =
                createSubpartitionMemoryReaderView();
        memoryTierReaderView.setMemoryTierReader(
                TestingTierReader.builder().setGetBacklogSupplier(() -> backlog).build());
        memoryTierReaderView.getNextBuffer();
        AvailabilityWithBacklog availabilityAndBacklog =
                memoryTierReaderView.getAvailabilityAndBacklog(0);
        assertThat(availabilityAndBacklog.getBacklog()).isEqualTo(backlog);
        // if credit is non-positive, only event can be available.
        assertThat(availabilityAndBacklog.isAvailable()).isFalse();
    }

    @Test
    void testGetAvailabilityAndBacklogNonPositiveCreditNextIsEvent() throws IOException {
        int backlog = 2;
        MemoryTierReaderView memoryTierReaderView =
                createSubpartitionMemoryReaderView();
        memoryTierReaderView.setMemoryTierReader(
                TestingTierReader.builder().setGetBacklogSupplier(() -> backlog).build());
        memoryTierReaderView.getNextBuffer();
        AvailabilityWithBacklog availabilityAndBacklog =
                memoryTierReaderView.getAvailabilityAndBacklog(0);
        assertThat(availabilityAndBacklog.getBacklog()).isEqualTo(backlog);
        assertThat(availabilityAndBacklog.isAvailable()).isFalse();
    }

    @Test
    void testRelease() throws Exception {
        MemoryTierReaderView memoryTierReaderView =
                createSubpartitionMemoryReaderView();
        CompletableFuture<Void> releaseDiskViewFuture = new CompletableFuture<>();
        TestingTierReader memoryTierReader =
                TestingTierReader.builder()
                        .setReleaseDataViewRunnable(() -> releaseDiskViewFuture.complete(null))
                        .build();
        memoryTierReaderView.setMemoryTierReader(memoryTierReader);
        memoryTierReaderView.releaseAllResources();
        assertThat(memoryTierReaderView.isReleased()).isTrue();
        assertThat(releaseDiskViewFuture).isCompleted();
    }

    @Test
    void testGetConsumingOffset() throws IOException {
        AtomicInteger nextBufferIndex = new AtomicInteger(0);
        MemoryTierReaderView memoryTierReaderView =
                createSubpartitionMemoryReaderView();
        TestingTierReader memoryTierReader =
                TestingTierReader.builder()
                        .setConsumeBufferFunction(
                                (toConsumeBuffer) ->
                                        Optional.of(
                                                createBufferAndBacklog(
                                                        0,
                                                        DataType.DATA_BUFFER,
                                                        nextBufferIndex.getAndIncrement())))
                        .build();
        memoryTierReaderView.setMemoryTierReader(memoryTierReader);
        assertThat(memoryTierReaderView.getConsumingOffset(true)).isEqualTo(-1);
        memoryTierReaderView.getNextBuffer();
        assertThat(memoryTierReaderView.getConsumingOffset(true)).isEqualTo(0);
        memoryTierReaderView.getNextBuffer();
        assertThat(memoryTierReaderView.getConsumingOffset(true)).isEqualTo(1);
    }

    private static MemoryTierReaderView createSubpartitionMemoryReaderView() {
        return new MemoryTierReaderView(new NoOpBufferAvailablityListener());
    }

    private static MemoryTierReaderView createSubpartitionMemoryReaderView(
            BufferAvailabilityListener bufferAvailabilityListener) {
        return new MemoryTierReaderView(bufferAvailabilityListener);
    }

    private static BufferAndBacklog createBufferAndBacklog(
            int buffersInBacklog, DataType nextDataType, int sequenceNumber) {
        int bufferSize = 8;
        Buffer buffer = TieredStoreTestUtils.createBuffer(bufferSize, true);
        return new BufferAndBacklog(buffer, buffersInBacklog, nextDataType, sequenceNumber, false);
    }
}
