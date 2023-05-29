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

import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.Buffer.DataType;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.NoOpBufferAvailablityListener;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.CreditBasedBufferQueueView;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.CreditBasedBufferQueueViewImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.BufferContext;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link CreditBasedBufferQueueView}. */
class NettyServiceViewTest {

    private static final String EXCEPTION_MESSAGE = "Excepted exception";

    private static final int SUBPARTITION_ID = 0;

    private static final int DEFAULT_BUFFER_SIZE = 0;

    @Test
    void testGetNextBuffer() throws IOException {
        NettyServiceViewBuilder nettyServiceViewBuilder = new NettyServiceViewBuilder();
        int bufferNumber = 1;
        DataType dataType = DataType.DATA_BUFFER;
        CreditBasedBufferQueueView creditBasedBufferQueueView =
                nettyServiceViewBuilder
                        .setBufferQueue(createBufferQueue(bufferNumber, dataType))
                        .build();
        Optional<Buffer> buffer = creditBasedBufferQueueView.getNextBuffer();
        assertThat(buffer.isPresent()).isTrue();
        assertThat(buffer.get().isBuffer()).isTrue();
        assertThat(buffer.get().getDataType()).isEqualTo(dataType);
    }

    @Test
    void testGetNextBufferThrowException() {
        NettyServiceViewBuilder nettyServiceViewBuilder = new NettyServiceViewBuilder();
        Throwable expectedException = new IOException(EXCEPTION_MESSAGE);
        CreditBasedBufferQueueView creditBasedBufferQueueView =
                nettyServiceViewBuilder
                        .setBufferQueue(createBufferQueueWithExpectedException(expectedException))
                        .build();
        assertThatThrownBy(creditBasedBufferQueueView::getNextBuffer)
                .hasStackTraceContaining(EXCEPTION_MESSAGE);
    }

    @Test
    void testGetNumberOfQueuedBuffers() {
        NettyServiceViewBuilder nettyServiceViewBuilder = new NettyServiceViewBuilder();
        int bufferNumber = 10;
        DataType dataType = DataType.DATA_BUFFER;
        CreditBasedBufferQueueView creditBasedBufferQueueView =
                nettyServiceViewBuilder
                        .setBufferQueue(createBufferQueue(bufferNumber, dataType))
                        .build();
        assertThat(creditBasedBufferQueueView.getBacklog()).isEqualTo(bufferNumber);
    }

    @Test
    void testGetNextBufferDataType() {
        NettyServiceViewBuilder nettyServiceViewBuilder = new NettyServiceViewBuilder();
        int bufferNumber = 1;
        DataType dataType = DataType.DATA_BUFFER;
        CreditBasedBufferQueueView creditBasedBufferQueueView =
                nettyServiceViewBuilder
                        .setBufferQueue(createBufferQueue(bufferNumber, dataType))
                        .build();
        assertThat(creditBasedBufferQueueView.getNextBufferDataType()).isEqualTo(dataType);
    }

    @Test
    void testNotifyDataAvailable() throws ExecutionException, InterruptedException {
        NettyServiceViewBuilder nettyServiceViewBuilder = new NettyServiceViewBuilder();
        CompletableFuture<Boolean> notifyReceiver = new CompletableFuture<>();
        CreditBasedBufferQueueView creditBasedBufferQueueView =
                nettyServiceViewBuilder
                        .setAvailabilityListener(
                                () -> {
                                    notifyReceiver.complete(true);
                                })
                        .build();
        creditBasedBufferQueueView.notifyDataAvailable();
        assertThat(notifyReceiver.get()).isTrue();
    }

    @Test
    void testRelease() throws ExecutionException, InterruptedException, IOException {
        NettyServiceViewBuilder nettyServiceViewBuilder = new NettyServiceViewBuilder();
        CompletableFuture<Boolean> releaseReceiver = new CompletableFuture<>();
        CreditBasedBufferQueueView creditBasedBufferQueueView =
                nettyServiceViewBuilder
                        .setReleaseNotifier(
                                () -> {
                                    releaseReceiver.complete(true);
                                })
                        .build();
        creditBasedBufferQueueView.release();
        assertThat(releaseReceiver.get()).isTrue();
    }

    private Queue<BufferContext> createBufferQueue(int bufferNumber, DataType dataType) {
        LinkedBlockingQueue<BufferContext> bufferQueue = new LinkedBlockingQueue<>();
        for (int i = 0; i < bufferNumber; ++i) {
            bufferQueue.add(
                    new BufferContext(
                            new NetworkBuffer(
                                    MemorySegmentFactory.allocateUnpooledSegment(
                                            DEFAULT_BUFFER_SIZE),
                                    FreeingBufferRecycler.INSTANCE,
                                    dataType,
                                    DEFAULT_BUFFER_SIZE),
                            i,
                            SUBPARTITION_ID));
        }
        return bufferQueue;
    }

    private Queue<BufferContext> createBufferQueueWithExpectedException(Throwable throwable) {
        LinkedBlockingQueue<BufferContext> bufferQueue = new LinkedBlockingQueue<>();
        bufferQueue.add(new BufferContext(throwable));
        return bufferQueue;
    }

    private static class NettyServiceViewBuilder {

        private Queue<BufferContext> bufferQueue = new LinkedBlockingQueue<>();
        private Runnable releaseNotifier = () -> {};
        private BufferAvailabilityListener availabilityListener =
                new NoOpBufferAvailablityListener();

        private NettyServiceViewBuilder() {}

        private NettyServiceViewBuilder setBufferQueue(Queue<BufferContext> bufferQueue) {
            this.bufferQueue = bufferQueue;
            return this;
        }

        private NettyServiceViewBuilder setReleaseNotifier(Runnable releaseNotifier) {
            this.releaseNotifier = releaseNotifier;
            return this;
        }

        private NettyServiceViewBuilder setAvailabilityListener(
                BufferAvailabilityListener availabilityListener) {
            this.availabilityListener = availabilityListener;
            return this;
        }

        private CreditBasedBufferQueueView build() {
            return new CreditBasedBufferQueueViewImpl(bufferQueue, availabilityListener, releaseNotifier);
        }
    }
}
