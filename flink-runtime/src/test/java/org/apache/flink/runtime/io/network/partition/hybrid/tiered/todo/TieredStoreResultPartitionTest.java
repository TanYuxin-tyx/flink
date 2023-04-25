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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.todo;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.executiongraph.ResultPartitionBytes;
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.disk.FileChannelManager;
import org.apache.flink.runtime.io.disk.FileChannelManagerImpl;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferHeader;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.NoOpBufferAvailablityListener;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.shuffle.TieredResultPartition;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.parseBufferHeader;
import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.throwCorruptDataException;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TieredResultPartition}. */
class TieredStoreResultPartitionTest {

    private static final int bufferSize = 1024;

    private static final int totalBuffers = 1000;

    private static final int totalBytes = 32 * 1024 * 1024;

    private static final int numThreads = 4;

    private FileChannelManager fileChannelManager;

    private NetworkBufferPool globalPool;

    private BatchShuffleReadBufferPool readBufferPool;

    private ScheduledExecutorService readIOExecutor;

    private TaskIOMetricGroup taskIOMetricGroup;

    @TempDir public java.nio.file.Path tempDataPath;

    @BeforeEach
    void before() {
        fileChannelManager =
                new FileChannelManagerImpl(new String[] {tempDataPath.toString()}, "testing");
        globalPool = new NetworkBufferPool(totalBuffers, bufferSize);
        readBufferPool = new BatchShuffleReadBufferPool(totalBytes, bufferSize);
        readIOExecutor =
                new ScheduledThreadPoolExecutor(
                        numThreads,
                        new ExecutorThreadFactory("test-io-scheduler-thread"),
                        (ignored, executor) -> {
                            if (executor.isShutdown()) {
                                // ignore rejected as shutdown.
                            } else {
                                throw new RejectedExecutionException();
                            }
                        });
    }

    @AfterEach
    void after() throws Exception {
        fileChannelManager.close();
        globalPool.destroy();
        readBufferPool.destroy();
        readIOExecutor.shutdown();
    }

    @Test
    void testMemoryEmit() throws Exception {
        int numBuffers = 100;
        int numSubpartitions = 10;
        int numRecords = 10;
        Random random = new Random();
        BufferPool bufferPool = globalPool.createBufferPool(numBuffers, numBuffers);

        try (TieredResultPartition partition =
                createTieredStoreResultPartition(numSubpartitions, bufferPool, false)) {
            Queue<Tuple2<ByteBuffer, Buffer.DataType>>[] dataWritten = new Queue[numSubpartitions];
            Queue<Buffer>[] buffersRead = new Queue[numSubpartitions];
            for (int i = 0; i < numSubpartitions; ++i) {
                dataWritten[i] = new ArrayDeque<>();
                buffersRead[i] = new ArrayDeque<>();
            }
            int[] numBytesWritten = new int[numSubpartitions];
            int[] numBytesRead = new int[numSubpartitions];
            Arrays.fill(numBytesWritten, 0);
            Arrays.fill(numBytesRead, 0);
            for (int i = 0; i < numRecords; ++i) {
                ByteBuffer record = generateRandomData(random.nextInt(2 * bufferSize) + 1, random);
                boolean isBroadCast = random.nextBoolean();
                if (isBroadCast) {
                    partition.broadcastRecord(record);
                    for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
                        recordDataWritten(
                                record,
                                dataWritten,
                                subpartition,
                                numBytesWritten,
                                Buffer.DataType.DATA_BUFFER);
                    }
                } else {
                    int subpartition = random.nextInt(numSubpartitions);
                    partition.emitRecord(record, subpartition);
                    recordDataWritten(
                            record,
                            dataWritten,
                            subpartition,
                            numBytesWritten,
                            Buffer.DataType.DATA_BUFFER);
                }
            }
            partition.finish();
            partition.close();
            for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
                ByteBuffer record = EventSerializer.toSerializedEvent(EndOfPartitionEvent.INSTANCE);
                recordDataWritten(
                        record,
                        dataWritten,
                        subpartition,
                        numBytesWritten,
                        Buffer.DataType.EVENT_BUFFER);
            }
            Tuple2<ResultSubpartitionView, TestingBufferAvailabilityListener>[] viewAndListeners =
                    createSubpartitionViews(partition, numSubpartitions);
            readData(
                    viewAndListeners,
                    (buffer, subpartitionId) -> {
                        int numBytes = buffer.readableBytes();
                        numBytesRead[subpartitionId] += numBytes;
                        MemorySegment segment =
                                MemorySegmentFactory.allocateUnpooledSegment(numBytes);
                        segment.put(0, buffer.getNioBufferReadable(), numBytes);
                        buffersRead[subpartitionId].add(
                                new NetworkBuffer(
                                        segment, (buf) -> {}, buffer.getDataType(), numBytes));
                    });
            checkWriteReadResult(
                    numSubpartitions, numBytesWritten, numBytesRead, dataWritten, buffersRead);
        }
    }

    //    @Test
    //    void testRemoteEmit() throws Exception {
    //        int initBuffers = 100;
    //        int numSubpartitions = 100;
    //        int numRecordsOfSubpartition = 10;
    //        int numBytesInASegment = bufferSize;
    //        int numBytesInARecord = bufferSize;
    //        Random random = new Random();
    //        BufferPool bufferPool = globalPool.createBufferPool(initBuffers, 2000);
    //        TieredStoreResultPartition tieredStoreResultPartition =
    //                createTieredStoreResultPartition(100, bufferPool, false, "DFS");
    ////        tieredStoreResultPartition.setNumBytesInASegment(numBytesInASegment);
    //        List<ByteBuffer> allByteBuffers = new ArrayList<>();
    //        for (int i = 0; i < numSubpartitions; ++i) {
    //            for (int j = 0; j < numRecordsOfSubpartition; ++j) {
    //                ByteBuffer record = generateRandomData(numBytesInARecord, random);
    //                tieredStoreResultPartition.emitRecord(record, i);
    //                allByteBuffers.add(record);
    //            }
    //        }
    //        // Check that the segment info is produced successfully.
    //        Tuple2[] viewAndListeners =
    //                createSubpartitionViews(tieredStoreResultPartition, numSubpartitions);
    //        readFromRemoteTier(viewAndListeners, numRecordsOfSubpartition);
    //        tieredStoreResultPartition.close();
    //        // Check that the shuffle data is produced correctly.
    //        int totalByteBufferIndex = 0;
    //        for (int i = 0; i < numSubpartitions; ++i) {
    //            for (int j = 0; j < numRecordsOfSubpartition; ++j) {
    //                Path shuffleDataPath =
    //                        tieredStoreResultPartition
    //                                .getBaseSubpartitionPath(i)
    //                                .get(0)
    //                                .suffix("/seg-" + j);
    //                List<ByteBuffer> dataToBuffers = readShuffleDataToBuffers(shuffleDataPath);
    //                assertThat(dataToBuffers).hasSize(1);
    //                assertThat(dataToBuffers.get(0).array())
    //                        .isEqualTo(allByteBuffers.get(totalByteBufferIndex++).array());
    //            }
    //        }
    //    }
    //
    //    @Test
    //    void testRemoteEmitLessThanASegment() throws Exception {
    //        int initBuffers = 100;
    //        int numSubpartitions = 1;
    //        int numBytesInASegment = 2 * bufferSize;
    //        int numBytesInARecord = bufferSize;
    //        Random random = new Random();
    //        BufferPool bufferPool = globalPool.createBufferPool(initBuffers, 2000);
    //        TieredStoreResultPartition tieredStoreResultPartition =
    //                createTieredStoreResultPartition(100, bufferPool, false, "DFS");
    ////        tieredStoreResultPartition.setNumBytesInASegment(numBytesInASegment);
    //        ByteBuffer record = generateRandomData(numBytesInARecord, random);
    //        tieredStoreResultPartition.emitRecord(record, 0);
    //        tieredStoreResultPartition.broadcastEvent(EndOfPartitionEvent.INSTANCE, false);
    //        // Check that the segment info is produced successfully.
    //        Tuple2[] viewAndListeners =
    //                createSubpartitionViews(tieredStoreResultPartition, numSubpartitions);
    //        readFromRemoteTier(viewAndListeners, 1);
    //        tieredStoreResultPartition.close();
    //        // Check that the shuffle data is produced correctly.
    //        Path shuffleDataPath =
    //                tieredStoreResultPartition.getBaseSubpartitionPath(0).get(0).suffix("/seg-" +
    // 0);
    //        List<ByteBuffer> dataToBuffers = readShuffleDataToBuffers(shuffleDataPath);
    //        assertThat(dataToBuffers).hasSize(2);
    //        assertThat(dataToBuffers.get(0).array()).isEqualTo(record.array());
    //        Buffer buffer = EventSerializer.toBuffer(EndOfPartitionEvent.INSTANCE, false);
    //
    // assertThat(dataToBuffers.get(1).array()).isEqualTo(buffer.getNioBufferReadable().array());
    //    }

    @Test
    void testBroadcastEvent() throws Exception {
        final int numBuffers = 1;
        BufferPool bufferPool = globalPool.createBufferPool(numBuffers, numBuffers);
        try (TieredResultPartition resultPartition =
                createTieredStoreResultPartition(2, bufferPool, false)) {
            resultPartition.broadcastEvent(EndOfPartitionEvent.INSTANCE, false);
            // broadcast event does not request buffer
            assertThat(bufferPool.getNumberOfAvailableMemorySegments()).isEqualTo(1);
            resultPartition.close();
            Tuple2<ResultSubpartitionView, TestingBufferAvailabilityListener>[] viewAndListeners =
                    createSubpartitionViews(resultPartition, 2);
            boolean[] receivedEvent = new boolean[2];
            readData(
                    viewAndListeners,
                    (buffer, subpartition) -> {
                        assertThat(buffer.getDataType().isEvent()).isTrue();
                        try {
                            AbstractEvent event =
                                    EventSerializer.fromSerializedEvent(
                                            buffer.readOnlySlice().getNioBufferReadable(),
                                            TieredStoreResultPartitionTest.class.getClassLoader());
                            assertThat(event).isInstanceOf(EndOfPartitionEvent.class);
                            receivedEvent[subpartition] = true;
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
            assertThat(receivedEvent).containsExactly(true, true);
        }
    }

    /** Test write and read data from single subpartition with multiple consumer. */
    @Test
    void testMultipleConsumer() throws Exception {
        final int numBuffers = 10;
        final int numRecords = 10;
        final int numConsumers = 2;
        final int targetChannel = 0;
        final Random random = new Random();

        BufferPool bufferPool = globalPool.createBufferPool(numBuffers, numBuffers);
        try (TieredResultPartition resultPartition =
                createTieredStoreResultPartition(2, bufferPool, false)) {
            List<ByteBuffer> dataWritten = new ArrayList<>();
            for (int i = 0; i < numRecords; i++) {
                ByteBuffer record = generateRandomData(bufferSize, random);
                resultPartition.emitRecord(record, targetChannel);
                dataWritten.add(record);
            }
            resultPartition.finish();
            resultPartition.close();

            Tuple2[] viewAndListeners =
                    createMultipleConsumerView(resultPartition, targetChannel, 2);

            List<List<Buffer>> dataRead = new ArrayList<>();
            for (int i = 0; i < numConsumers; i++) {
                dataRead.add(new ArrayList<>());
            }
            readData(
                    viewAndListeners,
                    (buffer, subpartition) -> {
                        int numBytes = buffer.readableBytes();
                        if (buffer.isBuffer()) {
                            MemorySegment segment =
                                    MemorySegmentFactory.allocateUnpooledSegment(numBytes);
                            segment.put(0, buffer.getNioBufferReadable(), numBytes);
                            dataRead.get(subpartition)
                                    .add(
                                            new NetworkBuffer(
                                                    segment,
                                                    (buf) -> {},
                                                    buffer.getDataType(),
                                                    numBytes));
                        }
                    });

            for (int i = 0; i < numConsumers; i++) {
                assertThat(dataWritten).hasSameSizeAs(dataRead.get(i));
                List<Buffer> readBufferList = dataRead.get(i);
                for (int j = 0; j < dataWritten.size(); j++) {
                    ByteBuffer bufferWritten = dataWritten.get(j);
                    bufferWritten.rewind();
                    Buffer bufferRead = readBufferList.get(j);
                    assertThat(bufferRead.getNioBufferReadable()).isEqualTo(bufferWritten);
                }
            }
        }
    }

    @Test
    void testBroadcastResultPartition() throws Exception {
        final int numBuffers = 10;
        final int numRecords = 10;
        final int numConsumers = 2;
        final int numSubpartitions = 2;
        final Random random = new Random();

        BufferPool bufferPool = globalPool.createBufferPool(numBuffers, numBuffers);
        try (TieredResultPartition resultPartition =
                createTieredStoreResultPartition(numSubpartitions, bufferPool, true)) {
            List<ByteBuffer> dataWritten = new ArrayList<>();
            for (int i = 0; i < numRecords; i++) {
                ByteBuffer record = generateRandomData(bufferSize, random);
                resultPartition.broadcastRecord(record);
                dataWritten.add(record);
            }
            resultPartition.finish();
            // flush all buffers to disk.
            resultPartition.close();

            Tuple2[] viewAndListeners = createSubpartitionViews(resultPartition, numSubpartitions);

            List<List<Buffer>> dataRead = new ArrayList<>();
            for (int i = 0; i < numConsumers; i++) {
                dataRead.add(new ArrayList<>());
            }
            readData(
                    viewAndListeners,
                    (buffer, subpartition) -> {
                        int numBytes = buffer.readableBytes();
                        if (buffer.isBuffer()) {
                            MemorySegment segment =
                                    MemorySegmentFactory.allocateUnpooledSegment(numBytes);
                            segment.put(0, buffer.getNioBufferReadable(), numBytes);
                            dataRead.get(subpartition)
                                    .add(
                                            new NetworkBuffer(
                                                    segment,
                                                    (buf) -> {},
                                                    buffer.getDataType(),
                                                    numBytes));
                        }
                    });

            for (int i = 0; i < numConsumers; i++) {
                assertThat(dataWritten).hasSameSizeAs(dataRead.get(i));
                List<Buffer> readBufferList = dataRead.get(i);
                for (int j = 0; j < dataWritten.size(); j++) {
                    ByteBuffer bufferWritten = dataWritten.get(j);
                    bufferWritten.rewind();
                    Buffer bufferRead = readBufferList.get(j);
                    assertThat(bufferRead.getNioBufferReadable()).isEqualTo(bufferWritten);
                }
            }
        }
    }

    @Test
    void testClose() throws Exception {
        final int numBuffers = 1;

        BufferPool bufferPool = globalPool.createBufferPool(numBuffers, numBuffers);
        TieredResultPartition partition = createTieredStoreResultPartition(1, bufferPool, false);

        partition.close();
        // receive data to closed partition will throw exception.
        assertThatThrownBy(() -> partition.emitRecord(ByteBuffer.allocate(bufferSize), 0));
    }

    @Test
    @Timeout(30)
    void testRelease() throws Exception {
        final int numSubpartitions = 2;
        final int numBuffers = 10;

        BufferPool bufferPool = globalPool.createBufferPool(numBuffers, numBuffers);
        TieredResultPartition partition =
                createTieredStoreResultPartition(numSubpartitions, bufferPool, false);

        partition.emitRecord(ByteBuffer.allocate(bufferSize * 5), 1);
        assertThat(bufferPool.bestEffortGetNumOfUsedBuffers()).isEqualTo(5);

        partition.close();
        assertThat(bufferPool.isDestroyed()).isTrue();

        partition.release();

        while (checkNotNull(fileChannelManager.getPaths()[0].listFiles()).length != 0) {
            Thread.sleep(10);
        }

        assertThat(totalBuffers).isEqualTo(globalPool.getNumberOfAvailableMemorySegments());
    }

    @Test
    void testCreateSubpartitionViewAfterRelease() throws Exception {
        final int numBuffers = 10;
        BufferPool bufferPool = globalPool.createBufferPool(numBuffers, numBuffers);
        TieredResultPartition resultPartition =
                createTieredStoreResultPartition(2, bufferPool, false);
        resultPartition.release();
        assertThatThrownBy(
                        () ->
                                resultPartition.createSubpartitionView(
                                        0, new NoOpBufferAvailablityListener()))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testCreateSubpartitionViewLostData() throws Exception {
        final int numBuffers = 10;
        BufferPool bufferPool = globalPool.createBufferPool(numBuffers, numBuffers);
        TieredResultPartition resultPartition =
                createTieredStoreResultPartition(2, bufferPool, false);
        IOUtils.deleteFilesRecursively(tempDataPath);
        assertThatThrownBy(
                        () ->
                                resultPartition.createSubpartitionView(
                                        0, new NoOpBufferAvailablityListener()))
                .isInstanceOf(PartitionNotFoundException.class);
    }

    @Test
    void testMetricsUpdate() throws Exception {
        BufferPool bufferPool = globalPool.createBufferPool(3, 3);
        try (TieredResultPartition partition =
                createTieredStoreResultPartition(2, bufferPool, false)) {
            partition.emitRecord(ByteBuffer.allocate(bufferSize), 0);
            partition.broadcastRecord(ByteBuffer.allocate(bufferSize));
            assertThat(taskIOMetricGroup.getNumBuffersOutCounter().getCount()).isEqualTo(3);
            IOMetrics ioMetrics = taskIOMetricGroup.createSnapshot();
            assertThat(ioMetrics.getResultPartitionBytes()).hasSize(1);
            ResultPartitionBytes partitionBytes =
                    ioMetrics.getResultPartitionBytes().values().iterator().next();
            assertThat(partitionBytes.getSubpartitionBytes())
                    .containsExactly((long) 2 * bufferSize, (long) bufferSize);
        }
    }

    @Test
    void testMemoryTierRegisterMultipleConsumer() throws Exception {
        BufferPool bufferPool = globalPool.createBufferPool(2, 2);
        try (TieredResultPartition partition =
                createTieredStoreResultPartition(2, bufferPool, false)) {
            partition.createSubpartitionView(0, new NoOpBufferAvailablityListener());
            assertThatThrownBy(
                            () ->
                                    partition.createSubpartitionView(
                                            0, new NoOpBufferAvailablityListener()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Memory Tier does not support multiple consumers");
        }
    }

    @Test
    void testLocalTierRegisterMultipleConsumer() throws Exception {
        BufferPool bufferPool = globalPool.createBufferPool(2, 2);
        try (TieredResultPartition partition =
                createTieredStoreResultPartition(2, bufferPool, false)) {
            partition.createSubpartitionView(0, new NoOpBufferAvailablityListener());
            assertThatNoException()
                    .isThrownBy(
                            () ->
                                    partition.createSubpartitionView(
                                            0, new NoOpBufferAvailablityListener()));
        }
    }

    @Test
    void testMetricsUpdateForBroadcastOnlyResultPartition() throws Exception {
        BufferPool bufferPool = globalPool.createBufferPool(3, 3);
        try (TieredResultPartition partition =
                createTieredStoreResultPartition(2, bufferPool, true)) {
            partition.broadcastRecord(ByteBuffer.allocate(bufferSize));
            assertThat(taskIOMetricGroup.getNumBuffersOutCounter().getCount()).isEqualTo(1);
            IOMetrics ioMetrics = taskIOMetricGroup.createSnapshot();
            assertThat(ioMetrics.getResultPartitionBytes()).hasSize(1);
            ResultPartitionBytes partitionBytes =
                    ioMetrics.getResultPartitionBytes().values().iterator().next();
            assertThat(partitionBytes.getSubpartitionBytes())
                    .containsExactly(bufferSize, bufferSize);
        }
    }

    private void recordDataWritten(
            ByteBuffer record,
            Queue<Tuple2<ByteBuffer, Buffer.DataType>>[] dataWritten,
            int subpartition,
            int[] numBytesWritten,
            Buffer.DataType dataType) {
        record.rewind();
        dataWritten[subpartition].add(Tuple2.of(record, dataType));
        numBytesWritten[subpartition] += record.remaining();
    }

    private List<ByteBuffer> readShuffleDataToBuffers(Path shuffleDataPath) throws IOException {
        FSDataInputStream inputStream = shuffleDataPath.getFileSystem().open(shuffleDataPath);
        ByteBuffer headerBuffer;
        ByteBuffer dataBuffer;
        List<ByteBuffer> dataBufferList = new ArrayList<>();
        while (true) {
            headerBuffer = ByteBuffer.wrap(new byte[8]);
            headerBuffer.order(ByteOrder.nativeOrder());
            headerBuffer.clear();
            int bufferHeaderResult = inputStream.read(headerBuffer.array());
            if (bufferHeaderResult == -1) {
                break;
            }
            final BufferHeader header;
            try {
                header = parseBufferHeader(headerBuffer);
            } catch (BufferUnderflowException | IllegalArgumentException e) {
                // buffer underflow if header buffer is undersized
                // IllegalArgumentException if size is outside memory segment size
                throwCorruptDataException();
                break;
            }
            dataBuffer = ByteBuffer.wrap(new byte[header.getLength()]);
            assertThat(header.getLength()).isGreaterThan(0);
            int dataBufferResult = inputStream.read(dataBuffer.array(), 0, header.getLength());
            assertThat(dataBufferResult).isNotEqualTo(-1);
            dataBufferList.add(dataBuffer);
        }
        return dataBufferList;
    }

    private void readFromRemoteTier(
            Tuple2<ResultSubpartitionView, TestingBufferAvailabilityListener>[] viewAndListeners,
            int expectSegmentIndex)
            throws Exception {
        // CheckedThread[] subpartitionViewThreads = new CheckedThread[viewAndListeners.length];
        // for (int i = 0; i < viewAndListeners.length; i++) {
        //    // start thread for each view.
        //    final int subpartition = i;
        //    CheckedThread subpartitionViewThread =
        //            new CheckedThread() {
        //                @Override
        //                public void go() throws Exception {
        //                    TieredStoreSubpartitionViewDelegate view =
        //                            (TieredStoreSubpartitionViewDelegate)
        //                                    viewAndListeners[subpartition].f0;
        //                    view.notifyRequiredSegmentId(Integer.MAX_VALUE);
        //                    while (true) {
        //                        view.getNextBuffer();
        //                        if (((TieredStoreResultSubpartitionView) view.getNettyService())
        //                                        .getRequiredSegmentId()
        //                                == expectSegmentIndex) {
        //                            break;
        //                        }
        //                    }
        //                }
        //            };
        //    subpartitionViewThreads[subpartition] = subpartitionViewThread;
        //    subpartitionViewThread.start();
        // }
        // for (CheckedThread thread : subpartitionViewThreads) {
        //    thread.sync();
        // }
    }

    private long readData(
            Tuple2<ResultSubpartitionView, TestingBufferAvailabilityListener>[] viewAndListeners,
            BiConsumer<Buffer, Integer> bufferProcessor)
            throws Exception {
        AtomicInteger dataSize = new AtomicInteger(0);
        AtomicInteger numEndOfPartitionEvents = new AtomicInteger(0);
        CheckedThread[] subpartitionViewThreads = new CheckedThread[viewAndListeners.length];
        for (int i = 0; i < viewAndListeners.length; i++) {
            // start thread for each view.
            final int subpartition = i;
            CheckedThread subpartitionViewThread =
                    new CheckedThread() {
                        @Override
                        public void go() throws Exception {
                            ResultSubpartitionView view = viewAndListeners[subpartition].f0;
                            view.notifyRequiredSegmentId(Integer.MAX_VALUE);
                            while (true) {
                                ResultSubpartition.BufferAndBacklog bufferAndBacklog =
                                        view.getNextBuffer();
                                if (bufferAndBacklog == null) {
                                    viewAndListeners[subpartition].f1.waitForData();
                                    continue;
                                }
                                Buffer buffer = bufferAndBacklog.buffer();
                                bufferProcessor.accept(buffer, subpartition);
                                dataSize.addAndGet(buffer.readableBytes());
                                buffer.recycleBuffer();

                                if (!buffer.isBuffer()) {
                                    numEndOfPartitionEvents.incrementAndGet();
                                    view.releaseAllResources();
                                    break;
                                }
                                if (bufferAndBacklog.getNextDataType() == Buffer.DataType.NONE) {
                                    viewAndListeners[subpartition].f1.waitForData();
                                }
                            }
                        }
                    };
            subpartitionViewThreads[subpartition] = subpartitionViewThread;
            subpartitionViewThread.start();
        }
        for (CheckedThread thread : subpartitionViewThreads) {
            thread.sync();
        }
        return dataSize.get();
    }

    private static ByteBuffer generateRandomData(int dataSize, Random random) {
        byte[] dataWritten = new byte[dataSize];
        random.nextBytes(dataWritten);
        return ByteBuffer.wrap(dataWritten);
    }

    private TieredResultPartition createTieredStoreResultPartition(
            int numSubpartitions, BufferPool bufferPool, boolean isBroadcastOnly)
            throws IOException {
        //TierStorage[] tierStorages = new TierStorage[0];
        //TieredStoreMemoryManager storeMemoryManager = new TestingTieredStoreMemoryManager();
        //BufferAccumulator bufferAccumulator =
        //        new BufferAccumulatorImpl(
        //                tierStorages,
        //                new TierProducerAgent[0],
        //                numSubpartitions,
        //                bufferSize,
        //                isBroadcastOnly,
        //                storeMemoryManager,
        //                null);
        //TieredStorageProducerClientImpl tieredStorageProducerClient =
        //        new TieredStorageProducerClientImpl(
        //                numSubpartitions, isBroadcastOnly, bufferAccumulator);
        //TieredResultPartition tieredResultPartition =
        //        new TieredResultPartition(
        //                "TieredStoreResultPartitionTest",
        //                0,
        //                new ResultPartitionID(),
        //                ResultPartitionType.HYBRID_SELECTIVE,
        //                numSubpartitions,
        //                numSubpartitions,
        //                new ResultPartitionManager(),
        //                tierStorages,
        //                storeMemoryManager,
        //                new CacheFlushManager(),
        //                new BufferCompressor(bufferSize, "LZ4"),
        //                tieredStorageProducerClient,
        //                () -> bufferPool);
        //taskIOMetricGroup =
        //        UnregisteredMetricGroups.createUnregisteredTaskMetricGroup().getIOMetricGroup();
        //tieredResultPartition.setup();
        //tieredResultPartition.setMetricGroup(taskIOMetricGroup);
        //return tieredResultPartition;
        return null;
    }

    private static void checkWriteReadResult(
            int numSubpartitions,
            int[] numBytesWritten,
            int[] numBytesRead,
            Queue<Tuple2<ByteBuffer, Buffer.DataType>>[] dataWritten,
            Queue<Buffer>[] buffersRead) {
        for (int subpartitionIndex = 0; subpartitionIndex < numSubpartitions; ++subpartitionIndex) {
            assertThat(numBytesWritten[subpartitionIndex])
                    .isEqualTo(numBytesRead[subpartitionIndex]);

            List<Tuple2<ByteBuffer, Buffer.DataType>> eventsWritten = new ArrayList<>();
            List<Buffer> eventsRead = new ArrayList<>();

            ByteBuffer subpartitionDataWritten =
                    ByteBuffer.allocate(numBytesWritten[subpartitionIndex]);
            for (Tuple2<ByteBuffer, Buffer.DataType> bufferDataTypeTuple :
                    dataWritten[subpartitionIndex]) {
                subpartitionDataWritten.put(bufferDataTypeTuple.f0);
                bufferDataTypeTuple.f0.rewind();
                if (bufferDataTypeTuple.f1.isEvent()) {
                    eventsWritten.add(bufferDataTypeTuple);
                }
            }

            ByteBuffer subpartitionDataRead = ByteBuffer.allocate(numBytesRead[subpartitionIndex]);
            for (Buffer buffer : buffersRead[subpartitionIndex]) {
                subpartitionDataRead.put(buffer.getNioBufferReadable());
                if (!buffer.isBuffer()) {
                    eventsRead.add(buffer);
                }
            }

            subpartitionDataWritten.flip();
            subpartitionDataRead.flip();
            assertThat(subpartitionDataWritten).isEqualTo(subpartitionDataRead);

            assertThat(eventsWritten.size()).isEqualTo(eventsRead.size());
            for (int i = 0; i < eventsWritten.size(); i++) {
                assertThat(eventsWritten.get(i).f1).isEqualTo(eventsRead.get(i).getDataType());
                assertThat(eventsWritten.get(i).f0)
                        .isEqualTo(eventsRead.get(i).getNioBufferReadable());
            }
        }
    }

    private Tuple2<ResultSubpartitionView, TestingBufferAvailabilityListener>[]
            createSubpartitionViews(TieredResultPartition partition, int numSubpartitions)
                    throws Exception {
        Tuple2<ResultSubpartitionView, TestingBufferAvailabilityListener>[] viewAndListeners =
                new Tuple2[numSubpartitions];
        for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
            TestingBufferAvailabilityListener listener = new TestingBufferAvailabilityListener();
            viewAndListeners[subpartition] =
                    Tuple2.of(partition.createSubpartitionView(subpartition, listener), listener);
        }
        return viewAndListeners;
    }

    /** Create multiple consumer and bufferAvailabilityListener for single subpartition. */
    private Tuple2<ResultSubpartitionView, TestingBufferAvailabilityListener>[]
            createMultipleConsumerView(
                    TieredResultPartition partition, int subpartitionId, int numConsumers)
                    throws Exception {
        Tuple2<ResultSubpartitionView, TestingBufferAvailabilityListener>[] viewAndListeners =
                new Tuple2[numConsumers];
        for (int consumer = 0; consumer < numConsumers; ++consumer) {
            TestingBufferAvailabilityListener listener = new TestingBufferAvailabilityListener();
            viewAndListeners[consumer] =
                    Tuple2.of(partition.createSubpartitionView(subpartitionId, listener), listener);
        }
        return viewAndListeners;
    }

    private static final class TestingBufferAvailabilityListener
            implements BufferAvailabilityListener {

        private int numNotifications;

        @Override
        public synchronized void notifyDataAvailable() {
            if (numNotifications == 0) {
                notifyAll();
            }
            ++numNotifications;
        }

        public synchronized void waitForData() throws InterruptedException {
            if (numNotifications == 0) {
                wait();
            }
            numNotifications = 0;
        }
    }
}
