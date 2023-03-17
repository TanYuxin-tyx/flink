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

package org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.Buffer.DataType;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.NoOpBufferAvailablityListener;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView.AvailabilityWithBacklog;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.TieredStoreTestUtils;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TierReaderView}. */
class TierReaderViewTest {

    @Test
    void testGetNextBuffer() throws IOException {
        TierReaderView tierReaderView = createTierReaderView();
        BufferAndBacklog bufferAndBacklog = createBufferAndBacklog(1, DataType.DATA_BUFFER, 0);
        TestingTierReader testingTierReader =
                TestingTierReader.builder()
                        .setConsumeBufferFunction(
                                (bufferToConsume) -> Optional.of(bufferAndBacklog))
                        .build();
        tierReaderView.setTierReader(testingTierReader);
        BufferAndBacklog nextBuffer = tierReaderView.getNextBuffer();
        assertThat(nextBuffer).isSameAs(bufferAndBacklog);
    }

    @Test
    void testGetNextDataTypeIsNone() throws IOException {
        TierReaderView tierReaderView = createTierReaderView();
        BufferAndBacklog bufferAndBacklog = createBufferAndBacklog(0, DataType.NONE, 0);
        TestingTierReader testingTierReader =
                TestingTierReader.builder()
                        .setConsumeBufferFunction(
                                (bufferToConsume) -> Optional.of(bufferAndBacklog))
                        .build();
        tierReaderView.setTierReader(testingTierReader);
        BufferAndBacklog nextBuffer = tierReaderView.getNextBuffer();
        assertThat(nextBuffer).isNotNull();
        assertThat(nextBuffer.buffer()).isSameAs(bufferAndBacklog.buffer());
        assertThat(nextBuffer.buffersInBacklog()).isEqualTo(bufferAndBacklog.buffersInBacklog());
        assertThat(nextBuffer.getSequenceNumber()).isEqualTo(bufferAndBacklog.getSequenceNumber());
        assertThat(nextBuffer.getNextDataType()).isEqualTo(DataType.NONE);
    }

    @Test
    void testGetNextBufferThrowException() {
        TierReaderView tierReaderView = createTierReaderView();
        TestingTierReader testingTierReader =
                TestingTierReader.builder()
                        .setConsumeBufferFunction(
                                (nextToConsume) -> {
                                    throw new RuntimeException("expected exception.");
                                })
                        .build();
        tierReaderView.setTierReader(testingTierReader);
        assertThatThrownBy(tierReaderView::getNextBuffer)
                .hasStackTraceContaining("expected exception.");
    }

    @Test
    void testGetNextBufferZeroBacklog() throws IOException {
        TierReaderView tierReaderView = createTierReaderView();
        int backlog = 0;
        BufferAndBacklog targetBufferAndBacklog =
                createBufferAndBacklog(backlog, DataType.DATA_BUFFER, 0);
        TestingTierReader testingTierReader =
                TestingTierReader.builder()
                        .setConsumeBufferFunction(
                                (bufferToConsume) -> Optional.of(targetBufferAndBacklog))
                        .build();
        tierReaderView.setTierReader(testingTierReader);
        assertThat(tierReaderView.getNextBuffer())
                .satisfies(
                        (bufferAndBacklog -> {
                            // backlog is reset to maximum backlog of memory and disk.
                            assertThat(bufferAndBacklog.buffersInBacklog()).isEqualTo(backlog);
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
        TierReaderView tierReaderView =
                createTierReaderView(() -> notifyAvailableFuture.complete(null));
        tierReaderView.setTierReader(TestingTierReader.NO_OP);
        tierReaderView.getNextBuffer();
        tierReaderView.notifyDataAvailable();
        assertThat(notifyAvailableFuture).isCompleted();
    }

    @Test
    void testNotifyDataAvailableNotNeedNotify() throws IOException {
        CompletableFuture<Void> notifyAvailableFuture = new CompletableFuture<>();
        TierReaderView tierReaderView =
                createTierReaderView(() -> notifyAvailableFuture.complete(null));
        tierReaderView.setTierReader(TestingTierReader.NO_OP);
        tierReaderView.getNextBuffer();
        tierReaderView.notifyDataAvailable();
        assertThat(notifyAvailableFuture).isCompleted();
    }

    @Test
    void testGetZeroBacklogNeedNotify() {
        CompletableFuture<Void> notifyAvailableFuture = new CompletableFuture<>();
        TierReaderView tierReaderView =
                createTierReaderView(() -> notifyAvailableFuture.complete(null));
        tierReaderView.setTierReader(
                TestingTierReader.builder().setGetBacklogSupplier(() -> 0).build());
        AvailabilityWithBacklog availabilityAndBacklog =
                tierReaderView.getAvailabilityAndBacklog(0);
        assertThat(availabilityAndBacklog.getBacklog()).isZero();
        assertThat(notifyAvailableFuture).isNotCompleted();
        tierReaderView.notifyDataAvailable();
        assertThat(notifyAvailableFuture).isCompleted();
    }

    @Test
    void testGetAvailabilityAndBacklogPositiveCredit() {
        TierReaderView tierReaderView = createTierReaderView();
        int backlog = 2;
        tierReaderView.setTierReader(
                TestingTierReader.builder().setGetBacklogSupplier(() -> backlog).build());
        AvailabilityWithBacklog availabilityAndBacklog =
                tierReaderView.getAvailabilityAndBacklog(1);
        assertThat(availabilityAndBacklog.getBacklog()).isEqualTo(backlog);
        // positive credit always available.
        assertThat(availabilityAndBacklog.isAvailable()).isTrue();
    }

    @Test
    void testGetAvailabilityAndBacklogNonPositiveCreditNextIsData() throws IOException {
        int backlog = 2;
        TierReaderView tierReaderView = createTierReaderView();
        tierReaderView.setTierReader(
                TestingTierReader.builder().setGetBacklogSupplier(() -> backlog).build());
        tierReaderView.getNextBuffer();
        AvailabilityWithBacklog availabilityAndBacklog =
                tierReaderView.getAvailabilityAndBacklog(0);
        assertThat(availabilityAndBacklog.getBacklog()).isEqualTo(backlog);
        // if credit is non-positive, only event can be available.
        assertThat(availabilityAndBacklog.isAvailable()).isFalse();
    }

    @Test
    void testGetAvailabilityAndBacklogNonPositiveCreditNextIsEvent() throws IOException {
        int backlog = 2;
        TierReaderView tierReaderView = createTierReaderView();
        tierReaderView.setTierReader(
                TestingTierReader.builder().setGetBacklogSupplier(() -> backlog).build());
        tierReaderView.getNextBuffer();
        AvailabilityWithBacklog availabilityAndBacklog =
                tierReaderView.getAvailabilityAndBacklog(0);
        assertThat(availabilityAndBacklog.getBacklog()).isEqualTo(backlog);
        assertThat(availabilityAndBacklog.isAvailable()).isFalse();
    }

    @Test
    void testRelease() throws Exception {
        TierReaderView tierReaderView = createTierReaderView();
        CompletableFuture<Void> releaseFuture = new CompletableFuture<>();
        TestingTierReader testingTierReader =
                TestingTierReader.builder()
                        .setReleaseDataViewRunnable(() -> releaseFuture.complete(null))
                        .build();
        tierReaderView.setTierReader(testingTierReader);
        tierReaderView.release();
        assertThat(tierReaderView.isReleased()).isTrue();
        assertThat(releaseFuture).isCompleted();
    }

    @Test
    void testGetConsumingOffset() throws IOException {
        AtomicInteger nextBufferIndex = new AtomicInteger(0);
        TierReaderView tierReaderView = createTierReaderView();
        TestingTierReader testingTierReader =
                TestingTierReader.builder()
                        .setConsumeBufferFunction(
                                (toConsumeBuffer) ->
                                        Optional.of(
                                                createBufferAndBacklog(
                                                        0,
                                                        DataType.DATA_BUFFER,
                                                        nextBufferIndex.getAndIncrement())))
                        .build();
        tierReaderView.setTierReader(testingTierReader);
        assertThat(tierReaderView.getConsumingOffset(true)).isEqualTo(-1);
        tierReaderView.getNextBuffer();
        assertThat(tierReaderView.getConsumingOffset(true)).isEqualTo(0);
        tierReaderView.getNextBuffer();
        assertThat(tierReaderView.getConsumingOffset(true)).isEqualTo(1);
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
        int bufferSize = 8;
        Buffer buffer = TieredStoreTestUtils.createBuffer(bufferSize, true);
        return new BufferAndBacklog(buffer, buffersInBacklog, nextDataType, sequenceNumber);
    }
}
