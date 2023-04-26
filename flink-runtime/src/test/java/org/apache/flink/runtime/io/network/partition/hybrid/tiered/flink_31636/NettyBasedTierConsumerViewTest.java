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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.flink_31636;

import org.apache.flink.runtime.io.network.buffer.Buffer.DataType;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.NoOpBufferAvailablityListener;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView.AvailabilityWithBacklog;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.service.NettyServiceView;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.service.NettyServiceViewImpl;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.flink_31636.NettyBufferQueueTest.createBufferAndBacklog;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link NettyServiceView}. */
class NettyBasedTierConsumerViewTest {

    @Test
    void testGetNextBuffer() throws IOException {
        NettyServiceView nettyServiceView = createNettyServiceView();
        BufferAndBacklog bufferAndBacklog = createBufferAndBacklog(1, DataType.DATA_BUFFER, 0);
        TestingNettyBufferQueue testingNettyBufferQueue =
                TestingNettyBufferQueue.builder()
                        .setConsumeBufferFunction(
                                (bufferToConsume) -> Optional.of(bufferAndBacklog))
                        .build();
        nettyServiceView.setNettyBufferQueue(testingNettyBufferQueue);
        BufferAndBacklog nextBuffer = nettyServiceView.getNextBuffer();
        assertThat(nextBuffer).isSameAs(bufferAndBacklog);
    }

    @Test
    void testGetNextDataTypeIsNone() throws IOException {
        NettyServiceView nettyServiceView = createNettyServiceView();
        BufferAndBacklog bufferAndBacklog = createBufferAndBacklog(0, DataType.NONE, 0);
        TestingNettyBufferQueue testingNettyBufferQueue =
                TestingNettyBufferQueue.builder()
                        .setConsumeBufferFunction(
                                (bufferToConsume) -> Optional.of(bufferAndBacklog))
                        .build();
        nettyServiceView.setNettyBufferQueue(testingNettyBufferQueue);
        BufferAndBacklog nextBuffer = nettyServiceView.getNextBuffer();
        assertThat(nextBuffer).isNotNull();
        assertThat(nextBuffer.buffer()).isSameAs(bufferAndBacklog.buffer());
        assertThat(nextBuffer.buffersInBacklog()).isEqualTo(bufferAndBacklog.buffersInBacklog());
        assertThat(nextBuffer.getSequenceNumber()).isEqualTo(bufferAndBacklog.getSequenceNumber());
        assertThat(nextBuffer.getNextDataType()).isEqualTo(DataType.NONE);
    }

    @Test
    void testGetNextBufferThrowException() {
        NettyServiceView nettyServiceView = createNettyServiceView();
        TestingNettyBufferQueue testingNettyBufferQueue =
                TestingNettyBufferQueue.builder()
                        .setConsumeBufferFunction(
                                (nextToConsume) -> {
                                    throw new RuntimeException("expected exception.");
                                })
                        .build();
        nettyServiceView.setNettyBufferQueue(testingNettyBufferQueue);
        assertThatThrownBy(nettyServiceView::getNextBuffer)
                .hasStackTraceContaining("expected exception.");
    }

    @Test
    void testGetNextBufferZeroBacklog() throws IOException {
        NettyServiceView nettyServiceView = createNettyServiceView();
        int backlog = 0;
        BufferAndBacklog targetBufferAndBacklog =
                createBufferAndBacklog(backlog, DataType.DATA_BUFFER, 0);
        TestingNettyBufferQueue testingNettyBufferQueue =
                TestingNettyBufferQueue.builder()
                        .setConsumeBufferFunction(
                                (bufferToConsume) -> Optional.of(targetBufferAndBacklog))
                        .build();
        nettyServiceView.setNettyBufferQueue(testingNettyBufferQueue);
        assertThat(nettyServiceView.getNextBuffer())
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
        NettyServiceView nettyServiceView =
                createNettyServiceView(() -> notifyAvailableFuture.complete(null));
        nettyServiceView.setNettyBufferQueue(TestingNettyBufferQueue.NO_OP);
        nettyServiceView.getNextBuffer();
        nettyServiceView.notifyDataAvailable();
        assertThat(notifyAvailableFuture).isCompleted();
    }

    @Test
    void testNotifyDataAvailableNotNeedNotify() throws IOException {
        CompletableFuture<Void> notifyAvailableFuture = new CompletableFuture<>();
        NettyServiceView nettyServiceView =
                createNettyServiceView(() -> notifyAvailableFuture.complete(null));
        nettyServiceView.setNettyBufferQueue(TestingNettyBufferQueue.NO_OP);
        nettyServiceView.getNextBuffer();
        nettyServiceView.notifyDataAvailable();
        assertThat(notifyAvailableFuture).isCompleted();
    }

    @Test
    void testGetZeroBacklogNeedNotify() {
        CompletableFuture<Void> notifyAvailableFuture = new CompletableFuture<>();
        NettyServiceView nettyServiceView =
                createNettyServiceView(() -> notifyAvailableFuture.complete(null));
        nettyServiceView.setNettyBufferQueue(
                TestingNettyBufferQueue.builder().setGetBacklogSupplier(() -> 0).build());
        AvailabilityWithBacklog availabilityAndBacklog =
                nettyServiceView.getAvailabilityAndBacklog(0);
        assertThat(availabilityAndBacklog.getBacklog()).isZero();
        assertThat(notifyAvailableFuture).isNotCompleted();
        nettyServiceView.notifyDataAvailable();
        assertThat(notifyAvailableFuture).isCompleted();
    }

    @Test
    void testGetAvailabilityAndBacklogPositiveCredit() {
        NettyServiceView nettyServiceView = createNettyServiceView();
        int backlog = 2;
        nettyServiceView.setNettyBufferQueue(
                TestingNettyBufferQueue.builder().setGetBacklogSupplier(() -> backlog).build());
        AvailabilityWithBacklog availabilityAndBacklog =
                nettyServiceView.getAvailabilityAndBacklog(1);
        assertThat(availabilityAndBacklog.getBacklog()).isEqualTo(backlog);
        // positive credit always available.
        assertThat(availabilityAndBacklog.isAvailable()).isTrue();
    }

    @Test
    void testGetAvailabilityAndBacklogNonPositiveCreditNextIsData() throws IOException {
        int backlog = 2;
        NettyServiceView nettyServiceView = createNettyServiceView();
        nettyServiceView.setNettyBufferQueue(
                TestingNettyBufferQueue.builder().setGetBacklogSupplier(() -> backlog).build());
        nettyServiceView.getNextBuffer();
        AvailabilityWithBacklog availabilityAndBacklog =
                nettyServiceView.getAvailabilityAndBacklog(0);
        assertThat(availabilityAndBacklog.getBacklog()).isEqualTo(backlog);
        // if credit is non-positive, only event can be available.
        assertThat(availabilityAndBacklog.isAvailable()).isFalse();
    }

    @Test
    void testGetAvailabilityAndBacklogNonPositiveCreditNextIsEvent() throws IOException {
        int backlog = 2;
        NettyServiceView nettyServiceView = createNettyServiceView();
        nettyServiceView.setNettyBufferQueue(
                TestingNettyBufferQueue.builder().setGetBacklogSupplier(() -> backlog).build());
        nettyServiceView.getNextBuffer();
        AvailabilityWithBacklog availabilityAndBacklog =
                nettyServiceView.getAvailabilityAndBacklog(0);
        assertThat(availabilityAndBacklog.getBacklog()).isEqualTo(backlog);
        assertThat(availabilityAndBacklog.isAvailable()).isFalse();
    }

    @Test
    void testUpdateNeedNotifyStatus() {
        NettyServiceView nettyServiceView = createNettyServiceView();
        nettyServiceView.notifyDataAvailable();
        assertThat(((NettyServiceViewImpl) nettyServiceView).getNeedNotifyStatus()).isFalse();
        nettyServiceView.updateNeedNotifyStatus();
        assertThat(((NettyServiceViewImpl) nettyServiceView).getNeedNotifyStatus()).isTrue();
    }

    @Test
    void testRelease() throws Exception {
        NettyServiceView nettyServiceView = createNettyServiceView();
        CompletableFuture<Void> releaseFuture = new CompletableFuture<>();
        TestingNettyBufferQueue testingNettyBufferQueue =
                TestingNettyBufferQueue.builder()
                        .setReleaseDataViewRunnable(() -> releaseFuture.complete(null))
                        .build();
        nettyServiceView.setNettyBufferQueue(testingNettyBufferQueue);
        nettyServiceView.release();
        assertThat(nettyServiceView.isReleased()).isTrue();
        assertThat(releaseFuture).isCompleted();
    }

    @Test
    void testGetConsumingOffset() throws IOException {
        AtomicInteger nextBufferIndex = new AtomicInteger(0);
        NettyServiceView nettyServiceView = createNettyServiceView();
        TestingNettyBufferQueue testingNettyBufferQueue =
                TestingNettyBufferQueue.builder()
                        .setConsumeBufferFunction(
                                (toConsumeBuffer) ->
                                        Optional.of(
                                                createBufferAndBacklog(
                                                        0,
                                                        DataType.DATA_BUFFER,
                                                        nextBufferIndex.getAndIncrement())))
                        .build();
        nettyServiceView.setNettyBufferQueue(testingNettyBufferQueue);
        assertThat(nettyServiceView.getConsumingOffset(true)).isEqualTo(-1);
        nettyServiceView.getNextBuffer();
        assertThat(nettyServiceView.getConsumingOffset(true)).isEqualTo(0);
        nettyServiceView.getNextBuffer();
        assertThat(nettyServiceView.getConsumingOffset(true)).isEqualTo(1);
    }

    private static NettyServiceView createNettyServiceView() {
        return new NettyServiceViewImpl(new NoOpBufferAvailablityListener());
    }

    private static NettyServiceView createNettyServiceView(
            BufferAvailabilityListener bufferAvailabilityListener) {
        return new NettyServiceViewImpl(bufferAvailabilityListener);
    }
}