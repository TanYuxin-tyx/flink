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

package org.apache.flink.runtime.io.network.partition.tieredstore.upstream.local.file;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;
import org.apache.flink.runtime.io.network.partition.NoOpBufferAvailablityListener;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.runtime.io.network.partition.tieredstore.TestingTierReader;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.TieredStoreConfiguration;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierReader;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierReaderViewId;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.local.memory.TestingSubpartitionDiskReaderViewOperation;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.DiskReaderManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.RegionBufferIndexTracker;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.RegionBufferIndexTrackerImpl;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.SubpartitionDiskReader;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.SubpartitionDiskReaderImpl;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.SubpartitionDiskReaderView;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.SubpartitionDiskReaderViewOperations;
import org.apache.flink.util.TestLoggerExtension;
import org.apache.flink.util.function.BiConsumerWithException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Optional;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DiskReaderManager}. */
@ExtendWith(TestLoggerExtension.class)
class DiskReaderManagerTest {
    private static final int BUFFER_SIZE = 1024;

    private static final int NUM_SUBPARTITIONS = 10;

    private static final int BUFFER_POOL_SIZE = 2;

    private final byte[] dataBytes = new byte[BUFFER_SIZE];

    private ManuallyTriggeredScheduledExecutorService ioExecutor;

    private BatchShuffleReadBufferPool bufferPool;

    private FileChannel dataFileChannel;

    private Path dataFilePath;

    private DiskReaderManager fileDataManager;

    private TestingSubpartitionDiskReaderViewOperation subpartitionViewOperation;

    private TestingSubpartitionDiskReader.Factory factory;

    @BeforeEach
    void before(@TempDir Path tempDir) throws IOException {
        Random random = new Random();
        random.nextBytes(dataBytes);
        bufferPool = new BatchShuffleReadBufferPool(BUFFER_POOL_SIZE * BUFFER_SIZE, BUFFER_SIZE);
        ioExecutor = new ManuallyTriggeredScheduledExecutorService();
        dataFilePath = Files.createFile(tempDir.resolve(".data"));
        dataFileChannel = openFileChannel(dataFilePath);
        factory = new TestingSubpartitionDiskReader.Factory();
        fileDataManager =
                new DiskReaderManager(
                        bufferPool,
                        ioExecutor,
                        new RegionBufferIndexTrackerImpl(NUM_SUBPARTITIONS),
                        dataFilePath,
                        factory,
                        TieredStoreConfiguration.builder(
                                        NUM_SUBPARTITIONS, bufferPool.getNumBuffersPerRequest())
                                .build(),
                        (subpartitionIndex, bufferIndex) -> false);
        subpartitionViewOperation = new TestingSubpartitionDiskReaderViewOperation();
    }

    @AfterEach
    void after() throws Exception {
        bufferPool.destroy();
        if (dataFileChannel != null) {
            dataFileChannel.close();
        }
    }

    // ----------------------- test run and register ---------------------------------------

    @Test
    void testRegisterReaderTriggerRun() throws Exception {
        TestingSubpartitionDiskReader reader = new TestingSubpartitionDiskReader();
        reader.setReadBuffersConsumer(
                (requestedBuffers, readBuffers) -> readBuffers.addAll(requestedBuffers));
        factory.allReaders.add(reader);

        assertThat(reader.readBuffers).isEmpty();

        fileDataManager.registerNewConsumer(0, TierReaderViewId.DEFAULT, subpartitionViewOperation);

        ioExecutor.trigger();

        assertThat(reader.readBuffers).hasSize(BUFFER_POOL_SIZE);
    }

    @Test
    void testBufferReleasedTriggerRun() throws Exception {
        TestingSubpartitionDiskReader reader = new TestingSubpartitionDiskReader();
        reader.setReadBuffersConsumer(
                (requestedBuffer, readBuffers) -> {
                    while (!requestedBuffer.isEmpty()) {
                        readBuffers.add(requestedBuffer.poll());
                    }
                });

        factory.allReaders.add(reader);

        fileDataManager.registerNewConsumer(0, TierReaderViewId.DEFAULT, subpartitionViewOperation);

        ioExecutor.trigger();

        assertThat(reader.readBuffers).hasSize(BUFFER_POOL_SIZE);
        assertThat(bufferPool.getAvailableBuffers()).isZero();

        fileDataManager.recycle(reader.readBuffers.poll());
        fileDataManager.recycle(reader.readBuffers.poll());

        // recycle buffer will push new runnable to ioExecutor.
        ioExecutor.trigger();
        assertThat(reader.readBuffers).hasSize(BUFFER_POOL_SIZE);
    }

    /** Test all not used buffers will be released after run method finish. */
    @Test
    void testRunReleaseUnusedBuffers() throws Exception {
        TestingSubpartitionDiskReader reader = new TestingSubpartitionDiskReader();

        CompletableFuture<Void> prepareForSchedulingFinished = new CompletableFuture<>();
        reader.setPrepareForSchedulingRunnable(() -> prepareForSchedulingFinished.complete(null));
        reader.setReadBuffersConsumer(
                (requestedBuffers, readBuffers) -> {
                    assertThat(prepareForSchedulingFinished).isCompleted();
                    assertThat(requestedBuffers).hasSize(BUFFER_POOL_SIZE);
                    assertThat(bufferPool.getAvailableBuffers()).isEqualTo(0);
                    // read one buffer, return another buffer to data manager.
                    readBuffers.add(requestedBuffers.poll());
                });
        factory.allReaders.add(reader);

        fileDataManager.registerNewConsumer(0, TierReaderViewId.DEFAULT, subpartitionViewOperation);

        ioExecutor.trigger();

        // not used buffer should be recycled.
        assertThat(bufferPool.getAvailableBuffers()).isEqualTo(1);
    }

    /** Test file data manager will schedule readers in order. */
    @Test
    void testScheduleReadersOrdered() throws Exception {
        TestingSubpartitionDiskReader reader1 = new TestingSubpartitionDiskReader();
        TestingSubpartitionDiskReader reader2 = new TestingSubpartitionDiskReader();

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

        reader1.setPriority(1);
        reader2.setPriority(2);

        factory.allReaders.add(reader1);
        factory.allReaders.add(reader2);

        fileDataManager.registerNewConsumer(0, TierReaderViewId.DEFAULT, subpartitionViewOperation);
        fileDataManager.registerNewConsumer(1, TierReaderViewId.DEFAULT, subpartitionViewOperation);

        // trigger run.
        ioExecutor.trigger();

        assertThat(readBuffersFinished2).isCompleted();
    }

    @Test
    void testRunRequestBufferTimeout() throws Exception {
        Duration bufferRequestTimeout = Duration.ofSeconds(3);

        // request all buffer first.
        bufferPool.requestBuffers();
        assertThat(bufferPool.getAvailableBuffers()).isZero();

        fileDataManager =
                new DiskReaderManager(
                        bufferPool,
                        ioExecutor,
                        new RegionBufferIndexTrackerImpl(NUM_SUBPARTITIONS),
                        dataFilePath,
                        factory,
                        TieredStoreConfiguration.builder(
                                        NUM_SUBPARTITIONS, bufferPool.getNumBuffersPerRequest())
                                .setBufferRequestTimeout(bufferRequestTimeout)
                                .build(),
                        (subpartitionIndex, bufferIndex) -> false);

        TestingSubpartitionDiskReader reader = new TestingSubpartitionDiskReader();
        CompletableFuture<Void> prepareForSchedulingFinished = new CompletableFuture<>();
        CompletableFuture<Throwable> cause = new CompletableFuture<>();
        reader.setPrepareForSchedulingRunnable(() -> prepareForSchedulingFinished.complete(null));
        reader.setFailConsumer((cause::complete));
        factory.allReaders.add(reader);

        fileDataManager.registerNewConsumer(0, TierReaderViewId.DEFAULT, subpartitionViewOperation);

        ioExecutor.trigger();

        assertThat(prepareForSchedulingFinished).isCompleted();
        assertThat(cause).isCompleted();
        assertThat(cause.get())
                .isInstanceOf(TimeoutException.class)
                .hasMessageContaining("Buffer request timeout");
    }

    /**
     * When {@link SubpartitionDiskReader#readBuffers(Queue, BufferRecycler)} throw IOException,
     * subpartition reader should fail.
     */
    @Test
    void testRunReadBuffersThrowException() throws Exception {
        TestingSubpartitionDiskReader reader = new TestingSubpartitionDiskReader();
        CompletableFuture<Throwable> cause = new CompletableFuture<>();
        reader.setFailConsumer((cause::complete));
        reader.setReadBuffersConsumer(
                (requestedBuffers, readBuffers) -> {
                    throw new IOException("expected exception.");
                });
        factory.allReaders.add(reader);

        fileDataManager.registerNewConsumer(0, TierReaderViewId.DEFAULT, subpartitionViewOperation);

        ioExecutor.trigger();

        assertThat(cause).isCompleted();
        assertThat(cause.get())
                .isInstanceOf(IOException.class)
                .hasMessageContaining("expected exception.");
    }

    // ----------------------- test release ---------------------------------------

    /** Test file data manager release when reader is reading buffers. */
    @Test
    @Timeout(10)
    @Disabled("Ignore the test temporally")
    void testReleasedWhenReading() throws Exception {
        TestingSubpartitionDiskReader reader = new TestingSubpartitionDiskReader();

        CompletableFuture<Throwable> cause = new CompletableFuture<>();
        reader.setFailConsumer((cause::complete));
        CompletableFuture<Void> readBufferStart = new CompletableFuture<>();
        CompletableFuture<Void> releasedFinish = new CompletableFuture<>();
        reader.setReadBuffersConsumer(
                (requestedBuffers, readBuffers) -> {
                    try {
                        readBufferStart.complete(null);
                        releasedFinish.get();
                    } catch (Exception e) {
                        // re-throw all exception to IOException caught by file data manager.
                        throw new IOException(e);
                    }
                });
        factory.allReaders.add(reader);

        CheckedThread releaseThread =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        readBufferStart.get();
                        fileDataManager.release();
                        releasedFinish.complete(null);
                    }
                };
        releaseThread.start();

        fileDataManager.registerNewConsumer(0, TierReaderViewId.DEFAULT, subpartitionViewOperation);

        ioExecutor.trigger();

        releaseThread.sync();

        assertThat(cause).isCompleted();
        assertThat(cause.get())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Result partition has been already released.");
    }

    /** Test file data manager was released, but receive new subpartition reader registration. */
    @Test
    void testRegisterSubpartitionReaderAfterReleased() {
        TestingSubpartitionDiskReader reader = new TestingSubpartitionDiskReader();
        factory.allReaders.add(reader);

        fileDataManager.release();
        assertThatThrownBy(
                        () -> {
                            fileDataManager.registerNewConsumer(
                                    0, TierReaderViewId.DEFAULT, subpartitionViewOperation);
                            ioExecutor.trigger();
                        })
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("HsFileDataManager is already released.");
    }

    /**
     * When the result partition fails, the view lock may be obtained when the LocalDiskDataManager
     * lock is held. In the same time, the downstream thread will acquire the lock of the
     * FileDataManager when acquiring the view lock. To avoid this deadlock, the logical of
     * subpartition view release subpartition reader and subpartition reader fail should not be
     * inside lock.
     */
    @Test
    @Disabled("Ignore the test temporally")
    void testConsumeWhileReleaseNoDeadlock() throws Exception {
        CompletableFuture<Void> consumerStart = new CompletableFuture<>();
        CompletableFuture<Void> readerFail = new CompletableFuture<>();
        SubpartitionDiskReaderView subpartitionView =
                new SubpartitionDiskReaderView(new NoOpBufferAvailablityListener());

        SubpartitionDiskReaderImpl subpartitionFileReader =
                new SubpartitionDiskReaderImpl(
                        0,
                        TierReaderViewId.DEFAULT,
                        dataFileChannel,
                        subpartitionView,
                        new RegionBufferIndexTrackerImpl(NUM_SUBPARTITIONS),
                        5,
                        fileDataManager::releaseSubpartitionReader,
                        (subpartitionIndex, bufferIndex) -> false,
                        BufferReaderWriterUtil.allocatedHeaderBuffer()) {
                    @Override
                    public synchronized void fail(Throwable failureCause) {
                        try {
                            readerFail.complete(null);
                            consumerStart.get();
                            super.fail(failureCause);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                };
        factory.allReaders.add(subpartitionFileReader);
        TierReader diskDataView =
                fileDataManager.registerNewConsumer(0, TierReaderViewId.DEFAULT, subpartitionView);
        subpartitionView.setDiskReader(diskDataView);
        TestingTierReader memoryDataView =
                TestingTierReader.builder()
                        .setConsumeBufferFunction(
                                (ignore) -> {
                                    // throw an exception to trigger the release of file reader.
                                    throw new RuntimeException("expected exception.");
                                })
                        .build();
        // subpartitionView.setMemoryDataView(memoryDataView);

        CheckedThread consumerThread =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        readerFail.get();
                        consumerStart.complete(null);
                        subpartitionView.getNextBuffer();
                    }
                };
        consumerThread.start();
        fileDataManager.release();
        consumerThread.sync();
    }

    private static FileChannel openFileChannel(Path path) throws IOException {
        return FileChannel.open(path, StandardOpenOption.READ);
    }

    private static class TestingSubpartitionDiskReader implements SubpartitionDiskReader {
        private Runnable prepareForSchedulingRunnable = () -> {};

        private BiConsumerWithException<Queue<MemorySegment>, Queue<MemorySegment>, IOException>
                readBuffersConsumer = (ignore1, ignore2) -> {};

        private Consumer<Throwable> failConsumer = (ignore) -> {};

        private Runnable releaseDataViewRunnable = () -> {};

        private final Queue<MemorySegment> readBuffers;

        private int priority;

        private TestingSubpartitionDiskReader() {
            this.readBuffers = new ArrayDeque<>();
        }

        @Override
        public void prepareForScheduling() {
            prepareForSchedulingRunnable.run();
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
        public void release() {}

        @Override
        public int compareTo(SubpartitionDiskReader that) {
            checkArgument(that instanceof TestingSubpartitionDiskReader);
            return Integer.compare(priority, ((TestingSubpartitionDiskReader) that).priority);
        }

        public void setPriority(int priority) {
            this.priority = priority;
        }

        public void setPrepareForSchedulingRunnable(Runnable prepareForSchedulingRunnable) {
            this.prepareForSchedulingRunnable = prepareForSchedulingRunnable;
        }

        public void setReadBuffersConsumer(
                BiConsumerWithException<Queue<MemorySegment>, Queue<MemorySegment>, IOException>
                        readBuffersConsumer) {
            this.readBuffersConsumer = readBuffersConsumer;
        }

        public void setFailConsumer(Consumer<Throwable> failConsumer) {
            this.failConsumer = failConsumer;
        }

        public void setReleaseDataViewRunnable(Runnable releaseDataViewRunnable) {
            this.releaseDataViewRunnable = releaseDataViewRunnable;
        }

        @Override
        public Optional<ResultSubpartition.BufferAndBacklog> consumeBuffer(
                int nextBufferToConsume, Queue<Buffer> errorBuffers) {
            return Optional.empty();
        }

        @Override
        public Buffer.DataType peekNextToConsumeDataType(
                int nextBufferToConsume, Queue<Buffer> errorBuffers) {
            return Buffer.DataType.NONE;
        }

        @Override
        public int getBacklog() {
            return 0;
        }

        @Override
        public void releaseDataView() {
            releaseDataViewRunnable.run();
        }

        /** Factory for {@link TestingSubpartitionDiskReader}. */
        private static class Factory implements SubpartitionDiskReader.Factory {
            private final Queue<SubpartitionDiskReader> allReaders = new ArrayDeque<>();

            @Override
            public SubpartitionDiskReader createFileReader(
                    int subpartitionId,
                    TierReaderViewId tierReaderViewId,
                    FileChannel dataFileChannel,
                    SubpartitionDiskReaderViewOperations operation,
                    RegionBufferIndexTracker dataIndex,
                    int maxBuffersReadAhead,
                    Consumer<SubpartitionDiskReader> fileReaderReleaser,
                    BiFunction<Integer, Integer, Boolean> isLastRecordInSegmentDecider,
                    ByteBuffer headerBuffer) {
                return checkNotNull(allReaders.poll());
            }
        }
    }
}