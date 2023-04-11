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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common;

import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.Buffer.DataType;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.NoOpBufferAvailablityListener;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView.AvailabilityWithBacklog;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link NettyBasedTierConsumerView}. */
class NettyBasedTierConsumerViewTest {

    @Test
    void testGetNextBuffer() throws IOException {
        NettyBasedTierConsumerView nettyBasedTierConsumerView = createNettyBasedTierConsumerView();
        BufferAndBacklog bufferAndBacklog = createBufferAndBacklog(1, DataType.DATA_BUFFER, 0);
        TestingNettyBasedTierConsumer testingNettyBasedTierConsumer =
                TestingNettyBasedTierConsumer.builder()
                        .setConsumeBufferFunction(
                                (bufferToConsume) -> Optional.of(bufferAndBacklog))
                        .build();
        nettyBasedTierConsumerView.setConsumer(testingNettyBasedTierConsumer);
        BufferAndBacklog nextBuffer = nettyBasedTierConsumerView.getNextBuffer();
        assertThat(nextBuffer).isSameAs(bufferAndBacklog);
    }

    @Test
    void testGetNextDataTypeIsNone() throws IOException {
        NettyBasedTierConsumerView nettyBasedTierConsumerView = createNettyBasedTierConsumerView();
        BufferAndBacklog bufferAndBacklog = createBufferAndBacklog(0, DataType.NONE, 0);
        TestingNettyBasedTierConsumer testingNettyBasedTierConsumer =
                TestingNettyBasedTierConsumer.builder()
                        .setConsumeBufferFunction(
                                (bufferToConsume) -> Optional.of(bufferAndBacklog))
                        .build();
        nettyBasedTierConsumerView.setConsumer(testingNettyBasedTierConsumer);
        BufferAndBacklog nextBuffer = nettyBasedTierConsumerView.getNextBuffer();
        assertThat(nextBuffer).isNotNull();
        assertThat(nextBuffer.buffer()).isSameAs(bufferAndBacklog.buffer());
        assertThat(nextBuffer.buffersInBacklog()).isEqualTo(bufferAndBacklog.buffersInBacklog());
        assertThat(nextBuffer.getSequenceNumber()).isEqualTo(bufferAndBacklog.getSequenceNumber());
        assertThat(nextBuffer.getNextDataType()).isEqualTo(DataType.NONE);
    }

    @Test
    void testGetNextBufferThrowException() {
        NettyBasedTierConsumerView nettyBasedTierConsumerView = createNettyBasedTierConsumerView();
        TestingNettyBasedTierConsumer testingNettyBasedTierConsumer =
                TestingNettyBasedTierConsumer.builder()
                        .setConsumeBufferFunction(
                                (nextToConsume) -> {
                                    throw new RuntimeException("expected exception.");
                                })
                        .build();
        nettyBasedTierConsumerView.setConsumer(testingNettyBasedTierConsumer);
        assertThatThrownBy(nettyBasedTierConsumerView::getNextBuffer)
                .hasStackTraceContaining("expected exception.");
    }

    @Test
    void testGetNextBufferZeroBacklog() throws IOException {
        NettyBasedTierConsumerView nettyBasedTierConsumerView = createNettyBasedTierConsumerView();
        int backlog = 0;
        BufferAndBacklog targetBufferAndBacklog =
                createBufferAndBacklog(backlog, DataType.DATA_BUFFER, 0);
        TestingNettyBasedTierConsumer testingNettyBasedTierConsumer =
                TestingNettyBasedTierConsumer.builder()
                        .setConsumeBufferFunction(
                                (bufferToConsume) -> Optional.of(targetBufferAndBacklog))
                        .build();
        nettyBasedTierConsumerView.setConsumer(testingNettyBasedTierConsumer);
        assertThat(nettyBasedTierConsumerView.getNextBuffer())
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
        NettyBasedTierConsumerView nettyBasedTierConsumerView =
                createNettyBasedTierConsumerView(() -> notifyAvailableFuture.complete(null));
        nettyBasedTierConsumerView.setConsumer(TestingNettyBasedTierConsumer.NO_OP);
        nettyBasedTierConsumerView.getNextBuffer();
        nettyBasedTierConsumerView.notifyDataAvailable();
        assertThat(notifyAvailableFuture).isCompleted();
    }

    @Test
    void testNotifyDataAvailableNotNeedNotify() throws IOException {
        CompletableFuture<Void> notifyAvailableFuture = new CompletableFuture<>();
        NettyBasedTierConsumerView nettyBasedTierConsumerView =
                createNettyBasedTierConsumerView(() -> notifyAvailableFuture.complete(null));
        nettyBasedTierConsumerView.setConsumer(TestingNettyBasedTierConsumer.NO_OP);
        nettyBasedTierConsumerView.getNextBuffer();
        nettyBasedTierConsumerView.notifyDataAvailable();
        assertThat(notifyAvailableFuture).isCompleted();
    }

    @Test
    void testGetZeroBacklogNeedNotify() {
        CompletableFuture<Void> notifyAvailableFuture = new CompletableFuture<>();
        NettyBasedTierConsumerView nettyBasedTierConsumerView =
                createNettyBasedTierConsumerView(() -> notifyAvailableFuture.complete(null));
        nettyBasedTierConsumerView.setConsumer(
                TestingNettyBasedTierConsumer.builder().setGetBacklogSupplier(() -> 0).build());
        AvailabilityWithBacklog availabilityAndBacklog =
                nettyBasedTierConsumerView.getAvailabilityAndBacklog(0);
        assertThat(availabilityAndBacklog.getBacklog()).isZero();
        assertThat(notifyAvailableFuture).isNotCompleted();
        nettyBasedTierConsumerView.notifyDataAvailable();
        assertThat(notifyAvailableFuture).isCompleted();
    }

    @Test
    void testGetAvailabilityAndBacklogPositiveCredit() {
        NettyBasedTierConsumerView nettyBasedTierConsumerView = createNettyBasedTierConsumerView();
        int backlog = 2;
        nettyBasedTierConsumerView.setConsumer(
                TestingNettyBasedTierConsumer.builder()
                        .setGetBacklogSupplier(() -> backlog)
                        .build());
        AvailabilityWithBacklog availabilityAndBacklog =
                nettyBasedTierConsumerView.getAvailabilityAndBacklog(1);
        assertThat(availabilityAndBacklog.getBacklog()).isEqualTo(backlog);
        // positive credit always available.
        assertThat(availabilityAndBacklog.isAvailable()).isTrue();
    }

    @Test
    void testGetAvailabilityAndBacklogNonPositiveCreditNextIsData() throws IOException {
        int backlog = 2;
        NettyBasedTierConsumerView nettyBasedTierConsumerView = createNettyBasedTierConsumerView();
        nettyBasedTierConsumerView.setConsumer(
                TestingNettyBasedTierConsumer.builder()
                        .setGetBacklogSupplier(() -> backlog)
                        .build());
        nettyBasedTierConsumerView.getNextBuffer();
        AvailabilityWithBacklog availabilityAndBacklog =
                nettyBasedTierConsumerView.getAvailabilityAndBacklog(0);
        assertThat(availabilityAndBacklog.getBacklog()).isEqualTo(backlog);
        // if credit is non-positive, only event can be available.
        assertThat(availabilityAndBacklog.isAvailable()).isFalse();
    }

    @Test
    void testGetAvailabilityAndBacklogNonPositiveCreditNextIsEvent() throws IOException {
        int backlog = 2;
        NettyBasedTierConsumerView nettyBasedTierConsumerView = createNettyBasedTierConsumerView();
        nettyBasedTierConsumerView.setConsumer(
                TestingNettyBasedTierConsumer.builder()
                        .setGetBacklogSupplier(() -> backlog)
                        .build());
        nettyBasedTierConsumerView.getNextBuffer();
        AvailabilityWithBacklog availabilityAndBacklog =
                nettyBasedTierConsumerView.getAvailabilityAndBacklog(0);
        assertThat(availabilityAndBacklog.getBacklog()).isEqualTo(backlog);
        assertThat(availabilityAndBacklog.isAvailable()).isFalse();
    }

    @Test
    void testUpdateNeedNotifyStatus() {
        NettyBasedTierConsumerView nettyBasedTierConsumerView = createNettyBasedTierConsumerView();
        nettyBasedTierConsumerView.notifyDataAvailable();
        assertThat(
                        ((NettyBasedTierConsumerViewImpl) nettyBasedTierConsumerView)
                                .getNeedNotifyStatus())
                .isFalse();
        nettyBasedTierConsumerView.updateNeedNotifyStatus();
        assertThat(
                        ((NettyBasedTierConsumerViewImpl) nettyBasedTierConsumerView)
                                .getNeedNotifyStatus())
                .isTrue();
    }

    @Test
    void testRelease() throws Exception {
        NettyBasedTierConsumerView nettyBasedTierConsumerView = createNettyBasedTierConsumerView();
        CompletableFuture<Void> releaseFuture = new CompletableFuture<>();
        TestingNettyBasedTierConsumer testingNettyBasedTierConsumer =
                TestingNettyBasedTierConsumer.builder()
                        .setReleaseDataViewRunnable(() -> releaseFuture.complete(null))
                        .build();
        nettyBasedTierConsumerView.setConsumer(testingNettyBasedTierConsumer);
        nettyBasedTierConsumerView.release();
        assertThat(nettyBasedTierConsumerView.isReleased()).isTrue();
        assertThat(releaseFuture).isCompleted();
    }

    @Test
    void testGetConsumingOffset() throws IOException {
        AtomicInteger nextBufferIndex = new AtomicInteger(0);
        NettyBasedTierConsumerView nettyBasedTierConsumerView = createNettyBasedTierConsumerView();
        TestingNettyBasedTierConsumer testingNettyBasedTierConsumer =
                TestingNettyBasedTierConsumer.builder()
                        .setConsumeBufferFunction(
                                (toConsumeBuffer) ->
                                        Optional.of(
                                                createBufferAndBacklog(
                                                        0,
                                                        DataType.DATA_BUFFER,
                                                        nextBufferIndex.getAndIncrement())))
                        .build();
        nettyBasedTierConsumerView.setConsumer(testingNettyBasedTierConsumer);
        assertThat(nettyBasedTierConsumerView.getConsumingOffset(true)).isEqualTo(-1);
        nettyBasedTierConsumerView.getNextBuffer();
        assertThat(nettyBasedTierConsumerView.getConsumingOffset(true)).isEqualTo(0);
        nettyBasedTierConsumerView.getNextBuffer();
        assertThat(nettyBasedTierConsumerView.getConsumingOffset(true)).isEqualTo(1);
    }

    private static NettyBasedTierConsumerView createNettyBasedTierConsumerView() {
        return new NettyBasedTierConsumerViewImpl(new NoOpBufferAvailablityListener());
    }

    private static NettyBasedTierConsumerView createNettyBasedTierConsumerView(
            BufferAvailabilityListener bufferAvailabilityListener) {
        return new NettyBasedTierConsumerViewImpl(bufferAvailabilityListener);
    }

    private static BufferAndBacklog createBufferAndBacklog(
            int buffersInBacklog, DataType nextDataType, int sequenceNumber) {
        int bufferSize = 8;
        Buffer buffer =
                new NetworkBuffer(
                        MemorySegmentFactory.allocateUnpooledSegment(bufferSize),
                        FreeingBufferRecycler.INSTANCE,
                        DataType.DATA_BUFFER,
                        bufferSize);
        return new BufferAndBacklog(buffer, buffersInBacklog, nextDataType, sequenceNumber);
    }
}