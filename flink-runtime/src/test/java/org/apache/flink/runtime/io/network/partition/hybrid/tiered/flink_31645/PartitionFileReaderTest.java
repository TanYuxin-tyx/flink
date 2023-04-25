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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.flink_31645;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.partition.NoOpBufferAvailablityListener;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file.PartitionFileReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file.ProducerMergePartitionFileReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file.ProducerMergePartitionSubpartitionReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.service.NettyBufferQueue;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.service.NettyServiceView;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.service.NettyServiceViewId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.service.NettyServiceViewImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.local.disk.RegionBufferIndexTrackerImpl;
import org.apache.flink.util.function.BiConsumerWithException;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PartitionFileReader}. */
class PartitionFileReaderTest {

    private static final int BUFFER_SIZE = 1024;

    private static final int NUM_SUBPARTITIONS = 10;

    private static final int BUFFER_POOL_SIZE = 2;

    private final byte[] dataBytes = new byte[BUFFER_SIZE];

    private BatchShuffleReadBufferPool bufferPool;

    private FileChannel dataFileChannel;

    private Path dataFilePath;

    private PartitionFileReader partitionFileReader;

    private NettyServiceView nettyServiceView;

    @BeforeEach
    void before(@TempDir Path tempDir) throws IOException {
        Random random = new Random();
        random.nextBytes(dataBytes);
        bufferPool = new BatchShuffleReadBufferPool(BUFFER_POOL_SIZE * BUFFER_SIZE, BUFFER_SIZE);
        dataFilePath = Files.createFile(tempDir.resolve(".data"));
        dataFileChannel = openFileChannel(dataFilePath);
        nettyServiceView = new NettyServiceViewImpl(new NoOpBufferAvailablityListener());
    }

    @AfterEach
    void after() throws Exception {
        bufferPool.destroy();
        if (dataFileChannel != null) {
            dataFileChannel.close();
        }
    }

    @Test
    void testProducerMergePartitionFileReaderRegisterNewConsumer() throws Exception {
        partitionFileReader = createProducerMergePartitionFileReader();
        NettyBufferQueue bufferQueue = partitionFileReader.createNettyBufferQueue(
                0,
                NettyServiceViewId.DEFAULT,
                nettyServiceView);
        partitionFileReader.read();
        int backlog = bufferQueue.getBacklog();
        System.out.println();
    }

    @Test
    void testBufferReleasedTriggerRun() throws Exception {
        partitionFileReader = createProducerMergePartitionFileReader();
        TestingProducerMergePartitionSubpartitionReader reader =
                new TestingProducerMergePartitionSubpartitionReader();
        reader.setReadBuffersConsumer(
                (requestedBuffer, readBuffers) -> {
                    while (!requestedBuffer.isEmpty()) {
                        readBuffers.add(requestedBuffer.poll());
                    }
                });
        partitionFileReader.createNettyBufferQueue(0, NettyServiceViewId.DEFAULT, nettyServiceView);
        assertThat(reader.readBuffers).hasSize(BUFFER_POOL_SIZE);
        assertThat(bufferPool.getAvailableBuffers()).isZero();
        ((ProducerMergePartitionFileReader) partitionFileReader).recycle(reader.readBuffers.poll());
        ((ProducerMergePartitionFileReader) partitionFileReader).recycle(reader.readBuffers.poll());
        // recycle buffer will push new runnable to ioExecutor.
        assertThat(reader.readBuffers).hasSize(BUFFER_POOL_SIZE);
    }

    /** Test all not used buffers will be released after run method finish. */
    @Test
    void testRunReleaseUnusedBuffers() throws Exception {
        partitionFileReader = createProducerMergePartitionFileReader();
        TestingProducerMergePartitionSubpartitionReader reader =
                new TestingProducerMergePartitionSubpartitionReader();
        CompletableFuture<Void> prepareForSchedulingFinished = new CompletableFuture<>();
        reader.setReadBuffersConsumer(
                (requestedBuffers, readBuffers) -> {
                    assertThat(prepareForSchedulingFinished).isCompleted();
                    assertThat(requestedBuffers).hasSize(BUFFER_POOL_SIZE);
                    assertThat(bufferPool.getAvailableBuffers()).isEqualTo(0);
                    // read one buffer, return another buffer to data manager.
                    readBuffers.add(requestedBuffers.poll());
                });
        partitionFileReader.createNettyBufferQueue(0, NettyServiceViewId.DEFAULT, nettyServiceView);
        // not used buffer should be recycled.
        assertThat(bufferPool.getAvailableBuffers()).isEqualTo(1);
    }

    /** Test file data manager will schedule readers in order. */
    @Test
    void testScheduleReadersOrdered() throws Exception {
        partitionFileReader = createProducerMergePartitionFileReader();
        TestingProducerMergePartitionSubpartitionReader reader1 =
                new TestingProducerMergePartitionSubpartitionReader();
        TestingProducerMergePartitionSubpartitionReader reader2 =
                new TestingProducerMergePartitionSubpartitionReader();
        CompletableFuture<Void> readBuffersFinished1 = new CompletableFuture<>();
        CompletableFuture<Void> readBuffersFinished2 = new CompletableFuture<>();
        reader1.setReadBuffersConsumer(
                (requestedBuffers, readBuffers) -> {
                    assertThat(readBuffersFinished2).isNotDone();
                    readBuffersFinished1.complete(null);
                });
        reader2.setReadBuffersConsumer(
                (requestedBuffers, readBuffers) -> {
                    assertThat(readBuffersFinished1).isDone();
                    readBuffersFinished2.complete(null);
                });
        partitionFileReader.createNettyBufferQueue(0, NettyServiceViewId.DEFAULT, nettyServiceView);
        partitionFileReader.createNettyBufferQueue(1, NettyServiceViewId.DEFAULT, nettyServiceView);
        // trigger run.
        assertThat(readBuffersFinished2).isCompleted();
    }

    @Test
    void testRunRequestBufferTimeout() throws Exception {
        partitionFileReader = createProducerMergePartitionFileReader();
        // request all buffer first.
        bufferPool.requestBuffers();
        assertThat(bufferPool.getAvailableBuffers()).isZero();
        TestingProducerMergePartitionSubpartitionReader reader =
                new TestingProducerMergePartitionSubpartitionReader();
        CompletableFuture<Void> prepareForSchedulingFinished = new CompletableFuture<>();
        CompletableFuture<Throwable> cause = new CompletableFuture<>();
        reader.setFailConsumer((cause::complete));
        partitionFileReader.createNettyBufferQueue(0, NettyServiceViewId.DEFAULT, nettyServiceView);
        assertThat(prepareForSchedulingFinished).isCompleted();
        assertThat(cause).isCompleted();
        assertThat(cause.get())
                .isInstanceOf(TimeoutException.class)
                .hasMessageContaining("Buffer request timeout");
    }

    @Test
    void testRunReadBuffersThrowException() throws Exception {
        partitionFileReader = createProducerMergePartitionFileReader();
        TestingProducerMergePartitionSubpartitionReader reader =
                new TestingProducerMergePartitionSubpartitionReader();
        CompletableFuture<Throwable> cause = new CompletableFuture<>();
        reader.setFailConsumer((cause::complete));
        reader.setReadBuffersConsumer(
                (requestedBuffers, readBuffers) -> {
                    throw new IOException("expected exception.");
                });
        partitionFileReader.createNettyBufferQueue(
                0, NettyServiceViewId.DEFAULT, nettyServiceView);
        assertThat(cause).isCompleted();
        assertThat(cause.get())
                .isInstanceOf(IOException.class)
                .hasMessageContaining("expected exception.");
    }

    @Test
    void testRegisterSubpartitionReaderAfterReleased() {
        partitionFileReader = createProducerMergePartitionFileReader();
        TestingProducerMergePartitionSubpartitionReader reader =
                new TestingProducerMergePartitionSubpartitionReader();
        partitionFileReader.release();
        assertThatThrownBy(
                        () -> {
                            partitionFileReader.createNettyBufferQueue(
                                    0, NettyServiceViewId.DEFAULT, nettyServiceView);
                        })
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("HsFileDataManager is already released.");
    }

    private PartitionFileReader createProducerMergePartitionFileReader() {
        return new ProducerMergePartitionFileReader(
                bufferPool,
                Executors.newScheduledThreadPool(1),
                new RegionBufferIndexTrackerImpl(NUM_SUBPARTITIONS),
                dataFilePath,
                bufferPool.getNumBuffersPerRequest(),
                Duration.ofSeconds(3),
                5);
    }

    private static FileChannel openFileChannel(Path path) throws IOException {
        return FileChannel.open(path, StandardOpenOption.READ);
    }

    private static class TestingProducerMergePartitionSubpartitionReader
            implements ProducerMergePartitionSubpartitionReader {

        private BiConsumerWithException<Queue<MemorySegment>, Queue<MemorySegment>, IOException>
                readBuffersConsumer = (ignore1, ignore2) -> {};

        private Consumer<Throwable> failConsumer = (ignore) -> {};

        private final Queue<MemorySegment> readBuffers;

        private int priority;

        private TestingProducerMergePartitionSubpartitionReader() {
            this.readBuffers = new ArrayDeque<>();
        }

        @Override
        public void readBuffers(Queue<MemorySegment> buffers, BufferRecycler recycler)
                throws IOException {
            readBuffersConsumer.accept(buffers, readBuffers);
        }

        @Override
        public void fail(Throwable failureCause) {
            failConsumer.accept(failureCause);
        }

        @Override
        public long getNextOffsetToLoad() {
            return 0;
        }

        public void setReadBuffersConsumer(
                BiConsumerWithException<Queue<MemorySegment>, Queue<MemorySegment>, IOException>
                        readBuffersConsumer) {
            this.readBuffersConsumer = readBuffersConsumer;
        }

        public void setFailConsumer(Consumer<Throwable> failConsumer) {
            this.failConsumer = failConsumer;
        }

        @Override
        public int compareTo(
                @NotNull
                        ProducerMergePartitionSubpartitionReader
                                producerMergePartitionSubpartitionReader) {
            return 0;
        }
    }
}
