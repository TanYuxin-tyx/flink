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
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.memory.SubpartitionMemoryReaderView;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SubpartitionMemoryReaderView}. */
class SubpartitionMemoryReaderViewTest {

    @Test
    void testGetNextBufferFromMemory() throws IOException {
        SubpartitionMemoryReaderView subpartitionMemoryReaderView =
                createSubpartitionMemoryReaderView();
        BufferAndBacklog bufferAndBacklog = createBufferAndBacklog(1, DataType.DATA_BUFFER, 0);
        TestingTierReader memoryTierReader =
                TestingTierReader.builder()
                        .setConsumeBufferFunction(
                                (bufferToConsume) -> Optional.of(bufferAndBacklog))
                        .build();
        subpartitionMemoryReaderView.setMemoryTierReader(memoryTierReader);
        BufferAndBacklog nextBuffer = subpartitionMemoryReaderView.getNextBuffer();
        assertThat(nextBuffer).isSameAs(bufferAndBacklog);
    }

    @Test
    void testGetNextBufferFromMemoryNextDataTypeIsNone() throws IOException {
        SubpartitionMemoryReaderView subpartitionMemoryReaderView =
                createSubpartitionMemoryReaderView();
        BufferAndBacklog bufferAndBacklog = createBufferAndBacklog(0, DataType.NONE, 0);
        TestingTierReader memoryTierReader =
                TestingTierReader.builder()
                        .setConsumeBufferFunction(
                                (bufferToConsume) -> Optional.of(bufferAndBacklog))
                        .build();
        subpartitionMemoryReaderView.setMemoryTierReader(memoryTierReader);
        BufferAndBacklog nextBuffer = subpartitionMemoryReaderView.getNextBuffer();
        assertThat(nextBuffer).isNotNull();
        assertThat(nextBuffer.buffer()).isSameAs(bufferAndBacklog.buffer());
        assertThat(nextBuffer.buffersInBacklog()).isEqualTo(bufferAndBacklog.buffersInBacklog());
        assertThat(nextBuffer.getSequenceNumber()).isEqualTo(bufferAndBacklog.getSequenceNumber());
        assertThat(nextBuffer.getNextDataType()).isEqualTo(DataType.NONE);
    }

    @Test
    void testGetNextBufferThrowException() {
        SubpartitionMemoryReaderView subpartitionMemoryReaderView =
                createSubpartitionMemoryReaderView();
        TestingTierReader memoryTierReader =
                TestingTierReader.builder()
                        .setConsumeBufferFunction(
                                (nextToConsume) -> {
                                    throw new RuntimeException("expected exception.");
                                })
                        .build();
        subpartitionMemoryReaderView.setMemoryTierReader(memoryTierReader);
        assertThatThrownBy(subpartitionMemoryReaderView::getNextBuffer)
                .hasStackTraceContaining("expected exception.");
    }

    @Test
    void testGetNextBufferZeroBacklog() throws IOException {
        SubpartitionMemoryReaderView subpartitionMemoryReaderView =
                createSubpartitionMemoryReaderView();
        int diskBacklog = 0;
        BufferAndBacklog targetBufferAndBacklog =
                createBufferAndBacklog(diskBacklog, DataType.DATA_BUFFER, 0);
        TestingTierReader memoryTierReader =
                TestingTierReader.builder()
                        .setConsumeBufferFunction(
                                (bufferToConsume) -> Optional.of(targetBufferAndBacklog))
                        .build();
        subpartitionMemoryReaderView.setMemoryTierReader(memoryTierReader);
        assertThat(subpartitionMemoryReaderView.getNextBuffer())
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
        SubpartitionMemoryReaderView subpartitionMemoryReaderView =
                createSubpartitionMemoryReaderView(() -> notifyAvailableFuture.complete(null));
        subpartitionMemoryReaderView.setMemoryTierReader(TestingTierReader.NO_OP);
        subpartitionMemoryReaderView.getNextBuffer();
        subpartitionMemoryReaderView.notifyDataAvailable();
        assertThat(notifyAvailableFuture).isCompleted();
    }

    @Test
    void testNotifyDataAvailableNotNeedNotify() throws IOException {
        CompletableFuture<Void> notifyAvailableFuture = new CompletableFuture<>();
        SubpartitionMemoryReaderView subpartitionMemoryReaderView =
                createSubpartitionMemoryReaderView(() -> notifyAvailableFuture.complete(null));
        subpartitionMemoryReaderView.setMemoryTierReader(TestingTierReader.NO_OP);
        subpartitionMemoryReaderView.getNextBuffer();
        subpartitionMemoryReaderView.notifyDataAvailable();
        assertThat(notifyAvailableFuture).isCompleted();
    }

    @Test
    void testGetZeroBacklogNeedNotify() {
        CompletableFuture<Void> notifyAvailableFuture = new CompletableFuture<>();
        SubpartitionMemoryReaderView subpartitionMemoryReaderView =
                createSubpartitionMemoryReaderView(() -> notifyAvailableFuture.complete(null));
        subpartitionMemoryReaderView.setMemoryTierReader(
                TestingTierReader.builder().setGetBacklogSupplier(() -> 0).build());
        AvailabilityWithBacklog availabilityAndBacklog =
                subpartitionMemoryReaderView.getAvailabilityAndBacklog(0);
        assertThat(availabilityAndBacklog.getBacklog()).isZero();
        assertThat(notifyAvailableFuture).isNotCompleted();
        subpartitionMemoryReaderView.notifyDataAvailable();
        assertThat(notifyAvailableFuture).isCompleted();
    }

    @Test
    void testGetAvailabilityAndBacklogPositiveCredit() {
        SubpartitionMemoryReaderView subpartitionMemoryReaderView =
                createSubpartitionMemoryReaderView();
        int backlog = 2;
        subpartitionMemoryReaderView.setMemoryTierReader(
                TestingTierReader.builder().setGetBacklogSupplier(() -> backlog).build());
        AvailabilityWithBacklog availabilityAndBacklog =
                subpartitionMemoryReaderView.getAvailabilityAndBacklog(1);
        assertThat(availabilityAndBacklog.getBacklog()).isEqualTo(backlog);
        // positive credit always available.
        assertThat(availabilityAndBacklog.isAvailable()).isTrue();
    }

    @Test
    void testGetAvailabilityAndBacklogNonPositiveCreditNextIsData() throws IOException {
        int backlog = 2;
        SubpartitionMemoryReaderView subpartitionMemoryReaderView =
                createSubpartitionMemoryReaderView();
        subpartitionMemoryReaderView.setMemoryTierReader(
                TestingTierReader.builder().setGetBacklogSupplier(() -> backlog).build());
        subpartitionMemoryReaderView.getNextBuffer();
        AvailabilityWithBacklog availabilityAndBacklog =
                subpartitionMemoryReaderView.getAvailabilityAndBacklog(0);
        assertThat(availabilityAndBacklog.getBacklog()).isEqualTo(backlog);
        // if credit is non-positive, only event can be available.
        assertThat(availabilityAndBacklog.isAvailable()).isFalse();
    }

    @Test
    void testGetAvailabilityAndBacklogNonPositiveCreditNextIsEvent() throws IOException {
        int backlog = 2;
        SubpartitionMemoryReaderView subpartitionMemoryReaderView =
                createSubpartitionMemoryReaderView();
        subpartitionMemoryReaderView.setMemoryTierReader(
                TestingTierReader.builder().setGetBacklogSupplier(() -> backlog).build());
        subpartitionMemoryReaderView.getNextBuffer();
        AvailabilityWithBacklog availabilityAndBacklog =
                subpartitionMemoryReaderView.getAvailabilityAndBacklog(0);
        assertThat(availabilityAndBacklog.getBacklog()).isEqualTo(backlog);
        assertThat(availabilityAndBacklog.isAvailable()).isFalse();
    }

    @Test
    void testRelease() throws Exception {
        SubpartitionMemoryReaderView subpartitionMemoryReaderView =
                createSubpartitionMemoryReaderView();
        CompletableFuture<Void> releaseDiskViewFuture = new CompletableFuture<>();
        TestingTierReader memoryTierReader =
                TestingTierReader.builder()
                        .setReleaseDataViewRunnable(() -> releaseDiskViewFuture.complete(null))
                        .build();
        subpartitionMemoryReaderView.setMemoryTierReader(memoryTierReader);
        subpartitionMemoryReaderView.releaseAllResources();
        assertThat(subpartitionMemoryReaderView.isReleased()).isTrue();
        assertThat(releaseDiskViewFuture).isCompleted();
    }

    @Test
    void testGetConsumingOffset() throws IOException {
        AtomicInteger nextBufferIndex = new AtomicInteger(0);
        SubpartitionMemoryReaderView subpartitionMemoryReaderView =
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
        subpartitionMemoryReaderView.setMemoryTierReader(memoryTierReader);
        assertThat(subpartitionMemoryReaderView.getConsumingOffset(true)).isEqualTo(-1);
        subpartitionMemoryReaderView.getNextBuffer();
        assertThat(subpartitionMemoryReaderView.getConsumingOffset(true)).isEqualTo(0);
        subpartitionMemoryReaderView.getNextBuffer();
        assertThat(subpartitionMemoryReaderView.getConsumingOffset(true)).isEqualTo(1);
    }

    private static SubpartitionMemoryReaderView createSubpartitionMemoryReaderView() {
        return new SubpartitionMemoryReaderView(new NoOpBufferAvailablityListener());
    }

    private static SubpartitionMemoryReaderView createSubpartitionMemoryReaderView(
            BufferAvailabilityListener bufferAvailabilityListener) {
        return new SubpartitionMemoryReaderView(bufferAvailabilityListener);
    }

    private static BufferAndBacklog createBufferAndBacklog(
            int buffersInBacklog, DataType nextDataType, int sequenceNumber) {
        int bufferSize = 8;
        Buffer buffer = TieredStoreTestUtils.createBuffer(bufferSize, true);
        return new BufferAndBacklog(buffer, buffersInBacklog, nextDataType, sequenceNumber, false);
    }
}
