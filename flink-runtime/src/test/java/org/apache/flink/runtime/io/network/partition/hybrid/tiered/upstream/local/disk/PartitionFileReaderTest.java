/// *
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
// package org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.local.disk;
//
// import org.apache.flink.core.memory.MemorySegment;
// import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutorService;
// import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
// import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
// import org.apache.flink.runtime.io.network.partition.NoOpBufferAvailablityListener;
// import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
// import
// org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoreConfiguration;
// import
// org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.local.disk.RegionBufferIndexTracker;
// import
// org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.local.disk.RegionBufferIndexTrackerImpl;
// import
// org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.service.NettyServiceView;
// import
// org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.service.NettyBasedTierConsumerViewId;
// import
// org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.service.NettyServiceViewImpl;
// import
// org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.common.file.PartitionFileReader;
// import
// org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.common.file.ProducerMergePartitionFileReader;
// import
// org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.common.file.ProducerMergePartitionTierConsumer;
// import org.apache.flink.util.function.BiConsumerWithException;
//
// import org.junit.jupiter.api.AfterEach;
// import org.junit.jupiter.api.BeforeEach;
// import org.junit.jupiter.api.Test;
// import org.junit.jupiter.api.io.TempDir;
//
// import java.io.IOException;
// import java.nio.ByteBuffer;
// import java.nio.channels.FileChannel;
// import java.nio.file.Files;
// import java.nio.file.Path;
// import java.nio.file.StandardOpenOption;
// import java.time.Duration;
// import java.util.ArrayDeque;
// import java.util.Optional;
// import java.util.Queue;
// import java.util.Random;
// import java.util.concurrent.CompletableFuture;
// import java.util.concurrent.TimeoutException;
// import java.util.function.Consumer;
//
// import static org.apache.flink.util.Preconditions.checkArgument;
// import static org.apache.flink.util.Preconditions.checkNotNull;
// import static org.assertj.core.api.Assertions.assertThat;
// import static org.assertj.core.api.Assertions.assertThatThrownBy;
//
/// ** Tests for {@link PartitionFileReader}. */
// class PartitionFileReaderTest {
//
//    private static final int BUFFER_SIZE = 1024;
//
//    private static final int NUM_SUBPARTITIONS = 10;
//
//    private static final int BUFFER_POOL_SIZE = 2;
//
//    private final byte[] dataBytes = new byte[BUFFER_SIZE];
//
//    private ManuallyTriggeredScheduledExecutorService ioExecutor;
//
//    private BatchShuffleReadBufferPool bufferPool;
//
//    private FileChannel dataFileChannel;
//
//    private Path dataFilePath;
//
//    private PartitionFileReader partitionFileReader;
//
//    private NettyServiceView nettyServiceView;
//
//    private TestingProducerMergePartitionTierConsumer.Factory factory;
//
//    @BeforeEach
//    void before(@TempDir Path tempDir) throws IOException {
//        Random random = new Random();
//        random.nextBytes(dataBytes);
//        bufferPool = new BatchShuffleReadBufferPool(BUFFER_POOL_SIZE * BUFFER_SIZE, BUFFER_SIZE);
//        ioExecutor = new ManuallyTriggeredScheduledExecutorService();
//        dataFilePath = Files.createFile(tempDir.resolve(".data"));
//        dataFileChannel = openFileChannel(dataFilePath);
//        factory = new TestingProducerMergePartitionTierConsumer.Factory();
//        nettyServiceView =
//                new NettyServiceViewImpl(new NoOpBufferAvailablityListener());
//    }
//
//    @AfterEach
//    void after() throws Exception {
//        bufferPool.destroy();
//        if (dataFileChannel != null) {
//            dataFileChannel.close();
//        }
//    }
//
//    @Test
//    void testProducerMergePartitionFileReaderRegisterNewConsumer() throws Exception {
//        partitionFileReader = createProducerMergePartitionFileReader();
//        TestingProducerMergePartitionTierConsumer reader =
//                new TestingProducerMergePartitionTierConsumer();
//        reader.setReadBuffersConsumer(
//                (requestedBuffers, loadDiskDataToBuffers) -> loadDiskDataToBuffers.addAll(requestedBuffers));
//        factory.allReaders.add(reader);
//        assertThat(reader.loadDiskDataToBuffers).isEmpty();
//        partitionFileReader.createNettyBufferQueue(
//                0, NettyBasedTierConsumerViewId.DEFAULT, nettyServiceView);
//        ioExecutor.trigger();
//        assertThat(reader.loadDiskDataToBuffers).hasSize(BUFFER_POOL_SIZE);
//    }
//
//    @Test
//    void testBufferReleasedTriggerRun() throws Exception {
//        partitionFileReader = createProducerMergePartitionFileReader();
//        TestingProducerMergePartitionTierConsumer reader =
//                new TestingProducerMergePartitionTierConsumer();
//        reader.setReadBuffersConsumer(
//                (requestedBuffer, loadDiskDataToBuffers) -> {
//                    while (!requestedBuffer.isEmpty()) {
//                        loadDiskDataToBuffers.add(requestedBuffer.poll());
//                    }
//                });
//        factory.allReaders.add(reader);
//        partitionFileReader.createNettyBufferQueue(
//                0, NettyBasedTierConsumerViewId.DEFAULT, nettyServiceView);
//        ioExecutor.trigger();
//        assertThat(reader.loadDiskDataToBuffers).hasSize(BUFFER_POOL_SIZE);
//        assertThat(bufferPool.getAvailableBuffers()).isZero();
//        ((ProducerMergePartitionFileReader)
// partitionFileReader).recycle(reader.loadDiskDataToBuffers.poll());
//        ((ProducerMergePartitionFileReader)
// partitionFileReader).recycle(reader.loadDiskDataToBuffers.poll());
//        // recycle buffer will push new runnable to ioExecutor.
//        ioExecutor.trigger();
//        assertThat(reader.loadDiskDataToBuffers).hasSize(BUFFER_POOL_SIZE);
//    }
//
//    /** Test all not used buffers will be released after run method finish. */
//    @Test
//    void testRunReleaseUnusedBuffers() throws Exception {
//        partitionFileReader = createProducerMergePartitionFileReader();
//        TestingProducerMergePartitionTierConsumer reader =
//                new TestingProducerMergePartitionTierConsumer();
//        CompletableFuture<Void> prepareForSchedulingFinished = new CompletableFuture<>();
//        reader.setPrepareForSchedulingRunnable(() -> prepareForSchedulingFinished.complete(null));
//        reader.setReadBuffersConsumer(
//                (requestedBuffers, loadDiskDataToBuffers) -> {
//                    assertThat(prepareForSchedulingFinished).isCompleted();
//                    assertThat(requestedBuffers).hasSize(BUFFER_POOL_SIZE);
//                    assertThat(bufferPool.getAvailableBuffers()).isEqualTo(0);
//                    // read one buffer, return another buffer to data manager.
//                    loadDiskDataToBuffers.add(requestedBuffers.poll());
//                });
//        factory.allReaders.add(reader);
//        partitionFileReader.createNettyBufferQueue(
//                0, NettyBasedTierConsumerViewId.DEFAULT, nettyServiceView);
//        ioExecutor.trigger();
//        // not used buffer should be recycled.
//        assertThat(bufferPool.getAvailableBuffers()).isEqualTo(1);
//    }
//
//    /** Test file data manager will schedule readers in order. */
//    @Test
//    void testScheduleReadersOrdered() throws Exception {
//        partitionFileReader = createProducerMergePartitionFileReader();
//        TestingProducerMergePartitionTierConsumer reader1 =
//                new TestingProducerMergePartitionTierConsumer();
//        TestingProducerMergePartitionTierConsumer reader2 =
//                new TestingProducerMergePartitionTierConsumer();
//        CompletableFuture<Void> readBuffersFinished1 = new CompletableFuture<>();
//        CompletableFuture<Void> readBuffersFinished2 = new CompletableFuture<>();
//        reader1.setReadBuffersConsumer(
//                (requestedBuffers, loadDiskDataToBuffers) -> {
//                    assertThat(readBuffersFinished2).isNotDone();
//                    readBuffersFinished1.complete(null);
//                });
//        reader2.setReadBuffersConsumer(
//                (requestedBuffers, loadDiskDataToBuffers) -> {
//                    assertThat(readBuffersFinished1).isDone();
//                    readBuffersFinished2.complete(null);
//                });
//        reader1.setPriority(1);
//        reader2.setPriority(2);
//        factory.allReaders.add(reader1);
//        factory.allReaders.add(reader2);
//        partitionFileReader.createNettyBufferQueue(
//                0, NettyBasedTierConsumerViewId.DEFAULT, nettyServiceView);
//        partitionFileReader.createNettyBufferQueue(
//                1, NettyBasedTierConsumerViewId.DEFAULT, nettyServiceView);
//        // trigger run.
//        ioExecutor.trigger();
//        assertThat(readBuffersFinished2).isCompleted();
//    }
//
//    @Test
//    void testRunRequestBufferTimeout() throws Exception {
//        partitionFileReader =
//                new ProducerMergePartitionFileReader(
//                        bufferPool,
//                        ioExecutor,
//                        new RegionBufferIndexTrackerImpl(NUM_SUBPARTITIONS),
//                        dataFilePath,
//                        factory,
//                        TieredStoreConfiguration.builder(
//                                        NUM_SUBPARTITIONS, bufferPool.getNumBuffersPerRequest())
//                                .setBufferRequestTimeout(Duration.ofSeconds(3))
//                                .build());
//        // request all buffer first.
//        bufferPool.requestBuffers();
//        assertThat(bufferPool.getAvailableBuffers()).isZero();
//        TestingProducerMergePartitionTierConsumer reader =
//                new TestingProducerMergePartitionTierConsumer();
//        CompletableFuture<Void> prepareForSchedulingFinished = new CompletableFuture<>();
//        CompletableFuture<Throwable> cause = new CompletableFuture<>();
//        reader.setPrepareForSchedulingRunnable(() -> prepareForSchedulingFinished.complete(null));
//        reader.setFailConsumer((cause::complete));
//        factory.allReaders.add(reader);
//        partitionFileReader.createNettyBufferQueue(
//                0, NettyBasedTierConsumerViewId.DEFAULT, nettyServiceView);
//        ioExecutor.trigger();
//        assertThat(prepareForSchedulingFinished).isCompleted();
//        assertThat(cause).isCompleted();
//        assertThat(cause.get())
//                .isInstanceOf(TimeoutException.class)
//                .hasMessageContaining("Buffer request timeout");
//    }
//
//    @Test
//    void testRunReadBuffersThrowException() throws Exception {
//        partitionFileReader = createProducerMergePartitionFileReader();
//        TestingProducerMergePartitionTierConsumer reader =
//                new TestingProducerMergePartitionTierConsumer();
//        CompletableFuture<Throwable> cause = new CompletableFuture<>();
//        reader.setFailConsumer((cause::complete));
//        reader.setReadBuffersConsumer(
//                (requestedBuffers, loadDiskDataToBuffers) -> {
//                    throw new IOException("expected exception.");
//                });
//        factory.allReaders.add(reader);
//        partitionFileReader.createNettyBufferQueue(
//                0, NettyBasedTierConsumerViewId.DEFAULT, nettyServiceView);
//        ioExecutor.trigger();
//        assertThat(cause).isCompleted();
//        assertThat(cause.get())
//                .isInstanceOf(IOException.class)
//                .hasMessageContaining("expected exception.");
//    }
//
//    @Test
//    void testRegisterSubpartitionReaderAfterReleased() {
//        partitionFileReader = createProducerMergePartitionFileReader();
//        TestingProducerMergePartitionTierConsumer reader =
//                new TestingProducerMergePartitionTierConsumer();
//        factory.allReaders.add(reader);
//        partitionFileReader.release();
//        assertThatThrownBy(
//                        () -> {
//                            partitionFileReader.createNettyBufferQueue(
//                                    0,
//                                    NettyBasedTierConsumerViewId.DEFAULT,
//                                    nettyServiceView);
//                            ioExecutor.trigger();
//                        })
//                .isInstanceOf(IllegalStateException.class)
//                .hasMessageContaining("HsFileDataManager is already released.");
//    }
//
//    private PartitionFileReader createProducerMergePartitionFileReader() {
//        return new ProducerMergePartitionFileReader(
//                bufferPool,
//                ioExecutor,
//                new RegionBufferIndexTrackerImpl(NUM_SUBPARTITIONS),
//                dataFilePath,
//                factory,
//                TieredStoreConfiguration.builder(
//                                NUM_SUBPARTITIONS, bufferPool.getNumBuffersPerRequest())
//                        .build());
//    }
//
//    private static FileChannel openFileChannel(Path path) throws IOException {
//        return FileChannel.open(path, StandardOpenOption.READ);
//    }
//
//    private static class TestingProducerMergePartitionTierConsumer
//            implements ProducerMergePartitionTierConsumer {
//
//        private Runnable prepareForSchedulingRunnable = () -> {};
//
//        private BiConsumerWithException<Queue<MemorySegment>, Queue<MemorySegment>, IOException>
//                readBuffersConsumer = (ignore1, ignore2) -> {};
//
//        private Consumer<Throwable> failConsumer = (ignore) -> {};
//
//        private Runnable releaseDataViewRunnable = () -> {};
//
//        private final Queue<MemorySegment> loadDiskDataToBuffers;
//
//        private int priority;
//
//        private TestingProducerMergePartitionTierConsumer() {
//            this.loadDiskDataToBuffers = new ArrayDeque<>();
//        }
//
//        @Override
//        public void prepareForScheduling() {
//            prepareForSchedulingRunnable.run();
//        }
//
//        @Override
//        public void loadDiskDataToBuffers(Queue<MemorySegment> buffers, BufferRecycler recycler)
//                throws IOException {
//            readBuffersConsumer.accept(buffers, loadDiskDataToBuffers);
//        }
//
//        @Override
//        public void fail(Throwable failureCause) {
//            failConsumer.accept(failureCause);
//        }
//
//        @Override
//        public void release() {
//            releaseDataViewRunnable.run();
//        }
//
//        @Override
//        public int compareTo(ProducerMergePartitionTierConsumer that) {
//            checkArgument(that instanceof TestingProducerMergePartitionTierConsumer);
//            return Integer.compare(
//                    priority, ((TestingProducerMergePartitionTierConsumer) that).priority);
//        }
//
//        public void setPriority(int priority) {
//            this.priority = priority;
//        }
//
//        public void setPrepareForSchedulingRunnable(Runnable prepareForSchedulingRunnable) {
//            this.prepareForSchedulingRunnable = prepareForSchedulingRunnable;
//        }
//
//        public void setReadBuffersConsumer(
//                BiConsumerWithException<Queue<MemorySegment>, Queue<MemorySegment>, IOException>
//                        readBuffersConsumer) {
//            this.readBuffersConsumer = readBuffersConsumer;
//        }
//
//        public void setFailConsumer(Consumer<Throwable> failConsumer) {
//            this.failConsumer = failConsumer;
//        }
//
//        public void setReleaseDataViewRunnable(Runnable releaseDataViewRunnable) {
//            this.releaseDataViewRunnable = releaseDataViewRunnable;
//        }
//
//        @Override
//        public Optional<ResultSubpartition.BufferAndBacklog> getNextBuffer(int bufferIndex) {
//            return Optional.empty();
//        }
//
//        @Override
//        public int getBacklog() {
//            return 0;
//        }
//
//        private static class Factory implements ProducerMergePartitionTierConsumer.Factory {
//            private final Queue<ProducerMergePartitionTierConsumer> allReaders = new
// ArrayDeque<>();
//
//            @Override
//            public ProducerMergePartitionTierConsumer createFileReader(
//                    int subpartitionId,
//                    NettyBasedTierConsumerViewId nettyBasedTierConsumerViewId,
//                    FileChannel dataFileChannel,
//                    NettyServiceView tierConsumerView,
//                    RegionBufferIndexTracker dataIndex,
//                    int maxBuffersReadAhead,
//                    Consumer<ProducerMergePartitionTierConsumer> fileReaderReleaser,
//                    ByteBuffer headerBuffer) {
//                return checkNotNull(allReaders.poll());
//            }
//        }
//    }
// }
