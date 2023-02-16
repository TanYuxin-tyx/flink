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

package org.apache.flink.runtime.io.network.partition.tieredstore;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferHeader;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.NoOpBufferAvailablityListener;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.TieredStoreConfiguration;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.TieredStoreResultPartition;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.parseBufferHeader;
import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.throwCorruptDataException;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/** Tests for the dfs tier in {@link TieredStoreResultPartition}. */
class TieredStoreResultPartitionDfsTest {

    private static final int bufferSize = 1024;

    private static final int totalBuffers = 2000;

    private NetworkBufferPool globalPool;

    private TaskIOMetricGroup taskIOMetricGroup;

    private TemporaryFolder tmpFolder;

    @BeforeEach
    void before() throws IOException {
        globalPool = new NetworkBufferPool(totalBuffers, bufferSize);
        tmpFolder = TemporaryFolder.builder().build();
        tmpFolder.create();
    }

    @AfterEach
    void after() {
        globalPool.destroy();
    }

    @Test
    void testSinglePartitionEmit() throws Exception {
        int initBuffers = 100;
        int numSubpartitions = 100;
        int numRecordsOfSubpartition = 10;
        int numBytesInASegment = bufferSize;
        int numBytesInARecord = bufferSize;
        Random random = new Random();
        BufferPool bufferPool = globalPool.createBufferPool(initBuffers, 2000);
        TieredStoreConfiguration configuration =
                TieredStoreConfiguration.builder(numSubpartitions, 0)
                        .setTieredStoreTiers("DFS")
                        .setTieredStoreSpillingType("FULL")
                        .setBaseDfsHomePath(getTempStorePathDir())
                        .build();
        TieredStoreResultPartition tieredStoreResultPartition =
                createTieredStoreResultPartitionOnlyDfs(
                        numSubpartitions, bufferPool, false, configuration);
        tieredStoreResultPartition.setNumBytesInASegment(numBytesInASegment);
        List<ByteBuffer> allByteBuffers = new ArrayList<>();
        for (int i = 0; i < numSubpartitions; ++i) {
            for (int j = 0; j < numRecordsOfSubpartition; ++j) {
                ByteBuffer record = generateRandomData(numBytesInARecord, random);
                tieredStoreResultPartition.emitRecord(record, i);
                allByteBuffers.add(record);
            }
        }
        // Check that the segment info is produced successfully.
        Tuple2[] viewAndListeners =
                createSubpartitionViews(tieredStoreResultPartition, numSubpartitions);
        long result =
                readSegmentInfoFromSubpartitionView(viewAndListeners, numRecordsOfSubpartition);
        assertThat(result).isEqualTo(numRecordsOfSubpartition * numSubpartitions * 12);
        tieredStoreResultPartition.close();
        // Check that the shuffle data is produced correctly.
        int totalByteBufferIndex = 0;
        for (int i = 0; i < numSubpartitions; ++i) {
            for (int j = 0; j < numRecordsOfSubpartition; ++j) {
                Path shuffleDataPath =
                        tieredStoreResultPartition
                                .getBaseSubpartitionPath(i)
                                .get(0)
                                .suffix("/seg-" + j);
                List<ByteBuffer> dataToBuffers = readShuffleDataToBuffers(shuffleDataPath);
                assertThat(dataToBuffers).hasSize(1);
                assertThat(dataToBuffers.get(0).array())
                        .isEqualTo(allByteBuffers.get(totalByteBufferIndex++).array());
            }
        }
    }

    @Test
    void testSinglePartitionEmitLessThanASegment() throws Exception {
        int initBuffers = 100;
        int numSubpartitions = 1;
        int numBytesInASegment = 2 * bufferSize;
        int numBytesInARecord = bufferSize;
        Random random = new Random();
        BufferPool bufferPool = globalPool.createBufferPool(initBuffers, 2000);
        TieredStoreConfiguration configuration =
                TieredStoreConfiguration.builder(numSubpartitions, 0)
                        .setTieredStoreTiers("DFS")
                        .setTieredStoreSpillingType("FULL")
                        .setBaseDfsHomePath(getTempStorePathDir())
                        .build();
        TieredStoreResultPartition tieredStoreResultPartition =
                createTieredStoreResultPartitionOnlyDfs(
                        numSubpartitions, bufferPool, false, configuration);
        tieredStoreResultPartition.setNumBytesInASegment(numBytesInASegment);
        ByteBuffer record = generateRandomData(numBytesInARecord, random);
        tieredStoreResultPartition.emitRecord(record, 0);
        tieredStoreResultPartition.broadcastEvent(EndOfPartitionEvent.INSTANCE, false);
        // Check that the segment info is produced successfully.
        Tuple2[] viewAndListeners =
                createSubpartitionViews(tieredStoreResultPartition, numSubpartitions);
        long result = readSegmentInfoFromSubpartitionView(viewAndListeners, 1);
        assertThat(result).isEqualTo(12);
        tieredStoreResultPartition.close();
        // Check that the shuffle data is produced correctly.
        Path shuffleDataPath =
                tieredStoreResultPartition.getBaseSubpartitionPath(0).get(0).suffix("/seg-" + 0);
        List<ByteBuffer> dataToBuffers = readShuffleDataToBuffers(shuffleDataPath);
        assertThat(dataToBuffers).hasSize(2);
        assertThat(dataToBuffers.get(0).array()).isEqualTo(record.array());
        Buffer buffer = EventSerializer.toBuffer(EndOfPartitionEvent.INSTANCE, false);
        assertThat(dataToBuffers.get(1).array()).isEqualTo(buffer.getNioBufferReadable().array());
    }

    @Test
    void testBroadcastRecordForNonOnlyBroadcast() throws Exception {
        int initBuffers = 100;
        int numSubpartitions = 100;
        int numRecordsOfSubpartition = 10;
        int numBytesInASegment = bufferSize;
        int numBytesInARecord = bufferSize;
        Random random = new Random();
        BufferPool bufferPool = globalPool.createBufferPool(initBuffers, 2000);
        TieredStoreConfiguration configuration =
                TieredStoreConfiguration.builder(numSubpartitions, 0)
                        .setTieredStoreTiers("DFS")
                        .setTieredStoreSpillingType("FULL")
                        .setBaseDfsHomePath(getTempStorePathDir())
                        .build();
        TieredStoreResultPartition tieredStoreResultPartition =
                createTieredStoreResultPartitionOnlyDfs(
                        numSubpartitions, bufferPool, false, configuration);
        tieredStoreResultPartition.setNumBytesInASegment(numBytesInASegment);
        List<ByteBuffer> allByteBuffers = new ArrayList<>();
        for (int i = 0; i < numRecordsOfSubpartition; ++i) {
            ByteBuffer record = generateRandomData(numBytesInARecord, random);
            tieredStoreResultPartition.broadcastRecord(record);
            for (int j = 0; j < numSubpartitions; ++j) {
                allByteBuffers.add(record);
            }
        }
        // Check that the segment info is produced successfully.
        Tuple2[] viewAndListeners =
                createSubpartitionViews(tieredStoreResultPartition, numSubpartitions);
        long result =
                readSegmentInfoFromSubpartitionView(viewAndListeners, numRecordsOfSubpartition);
        assertThat(result).isEqualTo(numRecordsOfSubpartition * numSubpartitions * 12);
        tieredStoreResultPartition.close();
        // Check that the shuffle data is produced correctly.
        int totalByteBufferIndex = 0;
        for (int i = 0; i < numRecordsOfSubpartition; ++i) {
            for (int j = 0; j < numSubpartitions; ++j) {
                Path shuffleDataPath =
                        tieredStoreResultPartition
                                .getBaseSubpartitionPath(j)
                                .get(0)
                                .suffix("/seg-" + i);
                List<ByteBuffer> dataToBuffers = readShuffleDataToBuffers(shuffleDataPath);
                assertThat(dataToBuffers).hasSize(1);
                assertThat(dataToBuffers.get(0).array())
                        .isEqualTo(allByteBuffers.get(totalByteBufferIndex++).array());
            }
        }
    }

    @Test
    void testBroadcastEventForNonOnlyBroadcast() throws Exception {
        int initBuffers = 100;
        int numSubpartitions = 100;
        int numRecordsOfSubpartition = 10;
        Buffer buffer = EventSerializer.toBuffer(EndOfPartitionEvent.INSTANCE, false);
        ByteBuffer serializedEvent = buffer.getNioBufferReadable();
        int numBytesInASegment = serializedEvent.array().length;
        BufferPool bufferPool = globalPool.createBufferPool(initBuffers, 2000);
        TieredStoreConfiguration configuration =
                TieredStoreConfiguration.builder(numSubpartitions, 0)
                        .setTieredStoreTiers("DFS")
                        .setTieredStoreSpillingType("FULL")
                        .setBaseDfsHomePath(getTempStorePathDir())
                        .build();
        TieredStoreResultPartition tieredStoreResultPartition =
                createTieredStoreResultPartitionOnlyDfs(
                        numSubpartitions, bufferPool, false, configuration);
        tieredStoreResultPartition.setNumBytesInASegment(numBytesInASegment);
        List<ByteBuffer> allByteBuffers = new ArrayList<>();
        for (int i = 0; i < numRecordsOfSubpartition; ++i) {
            tieredStoreResultPartition.broadcastEvent(EndOfPartitionEvent.INSTANCE, false);
            for (int j = 0; j < numSubpartitions; ++j) {
                allByteBuffers.add(serializedEvent);
            }
        }
        // Check that the segment info is produced successfully.
        Tuple2[] viewAndListeners =
                createSubpartitionViews(tieredStoreResultPartition, numSubpartitions);
        long result =
                readSegmentInfoFromSubpartitionView(viewAndListeners, numRecordsOfSubpartition);
        assertThat(result).isEqualTo(numRecordsOfSubpartition * numSubpartitions * 12);
        tieredStoreResultPartition.close();
        // Check that the shuffle data is produced correctly.
        int totalByteBufferIndex = 0;
        for (int i = 0; i < numRecordsOfSubpartition; ++i) {
            for (int j = 0; j < numSubpartitions; ++j) {
                Path shuffleDataPath =
                        tieredStoreResultPartition
                                .getBaseSubpartitionPath(j)
                                .get(0)
                                .suffix("/seg-" + i);
                List<ByteBuffer> dataToBuffers = readShuffleDataToBuffers(shuffleDataPath);
                assertThat(dataToBuffers).hasSize(1);
                assertThat(dataToBuffers.get(0).array())
                        .isEqualTo(allByteBuffers.get(totalByteBufferIndex++).array());
            }
        }
    }

    @Test
    void testBroadcastRecordForOnlyBroadcast() throws Exception {
        int initBuffers = 100;
        int numSubpartitions = 100;
        int numRecordsOfSubpartition = 10;
        int numBytesInASegment = bufferSize;
        int numBytesInARecord = bufferSize;
        Random random = new Random();
        BufferPool bufferPool = globalPool.createBufferPool(initBuffers, 2000);
        TieredStoreConfiguration configuration =
                TieredStoreConfiguration.builder(numSubpartitions, 0)
                        .setTieredStoreTiers("DFS")
                        .setTieredStoreSpillingType("FULL")
                        .setBaseDfsHomePath(getTempStorePathDir())
                        .build();
        TieredStoreResultPartition tieredStoreResultPartition =
                createTieredStoreResultPartitionOnlyDfs(
                        numSubpartitions, bufferPool, true, configuration);
        tieredStoreResultPartition.setNumBytesInASegment(numBytesInASegment);
        List<ByteBuffer> allByteBuffers = new ArrayList<>();
        for (int i = 0; i < numRecordsOfSubpartition; ++i) {
            ByteBuffer record = generateRandomData(numBytesInARecord, random);
            tieredStoreResultPartition.broadcastRecord(record);
            allByteBuffers.add(record);
        }
        // Check that the segment info is produced successfully.
        Tuple2[] viewAndListeners =
                createSubpartitionViews(tieredStoreResultPartition, numSubpartitions);
        long result =
                readSegmentInfoFromSubpartitionView(viewAndListeners, numRecordsOfSubpartition);
        assertThat(result).isEqualTo(numRecordsOfSubpartition * numSubpartitions * 12);
        tieredStoreResultPartition.close();
        // Check that the shuffle data is produced correctly.
        for (int i = 0; i < numRecordsOfSubpartition; ++i) {
            Path shuffleDataPath =
                    tieredStoreResultPartition
                            .getBaseSubpartitionPath(0)
                            .get(0)
                            .suffix("/seg-" + i);
            List<ByteBuffer> dataToBuffers = readShuffleDataToBuffers(shuffleDataPath);
            assertThat(dataToBuffers).hasSize(1);
            assertThat(dataToBuffers.get(0).array()).isEqualTo(allByteBuffers.get(i).array());
        }
    }

    @Test
    void testBroadcastEventForOnlyBroadcast() throws Exception {
        int initBuffers = 100;
        int numSubpartitions = 100;
        int numRecordsOfSubpartition = 10;
        Buffer buffer = EventSerializer.toBuffer(EndOfPartitionEvent.INSTANCE, false);
        ByteBuffer serializedEvent = buffer.getNioBufferReadable();
        int numBytesInASegment = serializedEvent.array().length;
        BufferPool bufferPool = globalPool.createBufferPool(initBuffers, 2000);
        TieredStoreConfiguration configuration =
                TieredStoreConfiguration.builder(numSubpartitions, 0)
                        .setTieredStoreTiers("DFS")
                        .setTieredStoreSpillingType("FULL")
                        .setBaseDfsHomePath(getTempStorePathDir())
                        .build();
        TieredStoreResultPartition tieredStoreResultPartition =
                createTieredStoreResultPartitionOnlyDfs(
                        numSubpartitions, bufferPool, true, configuration);
        tieredStoreResultPartition.setNumBytesInASegment(numBytesInASegment);
        List<ByteBuffer> allByteBuffers = new ArrayList<>();
        for (int i = 0; i < numRecordsOfSubpartition; ++i) {
            tieredStoreResultPartition.broadcastEvent(EndOfPartitionEvent.INSTANCE, false);
            allByteBuffers.add(serializedEvent);
        }
        // Check that the segment info is produced successfully.
        Tuple2[] viewAndListeners =
                createSubpartitionViews(tieredStoreResultPartition, numSubpartitions);
        long result =
                readSegmentInfoFromSubpartitionView(viewAndListeners, numRecordsOfSubpartition);
        assertThat(result).isEqualTo(numRecordsOfSubpartition * numSubpartitions * 12);
        tieredStoreResultPartition.close();
        // Check that the shuffle data is produced correctly.
        for (int i = 0; i < numRecordsOfSubpartition; ++i) {
            Path shuffleDataPath =
                    tieredStoreResultPartition
                            .getBaseSubpartitionPath(0)
                            .get(0)
                            .suffix("/seg-" + i);
            List<ByteBuffer> dataToBuffers = readShuffleDataToBuffers(shuffleDataPath);
            assertThat(dataToBuffers).hasSize(1);
            assertThat(dataToBuffers.get(0).array()).isEqualTo(allByteBuffers.get(i).array());
        }
    }

    @Test
    void testClose() throws Exception {
        final int numBuffers = 1;
        BufferPool bufferPool = globalPool.createBufferPool(numBuffers, numBuffers);
        TieredStoreConfiguration configuration =
                TieredStoreConfiguration.builder(1, 0)
                        .setTieredStoreTiers("DFS")
                        .setTieredStoreSpillingType("FULL")
                        .setBaseDfsHomePath(getTempStorePathDir())
                        .build();
        TieredStoreResultPartition tieredStoreResultPartition =
                createTieredStoreResultPartitionOnlyDfs(1, bufferPool, false, configuration);
        tieredStoreResultPartition.close();
        // emit data to closed partition will throw exception.
        assertThatThrownBy(
                () -> tieredStoreResultPartition.emitRecord(ByteBuffer.allocate(bufferSize), 0));
    }

    @Test
    void testRelease() throws Exception {
        final int numSubpartitions = 2;
        final int numBuffers = 10;
        final int numBytesInASegment = bufferSize * 10;

        BufferPool bufferPool = globalPool.createBufferPool(numBuffers, numBuffers);
        TieredStoreConfiguration configuration =
                TieredStoreConfiguration.builder(numSubpartitions, 0)
                        .setTieredStoreTiers("DFS")
                        .setTieredStoreSpillingType("FULL")
                        .setBaseDfsHomePath(getTempStorePathDir())
                        .build();
        TieredStoreResultPartition tieredStoreResultPartition =
                createTieredStoreResultPartitionOnlyDfs(
                        numSubpartitions, bufferPool, false, configuration);
        tieredStoreResultPartition.setNumBytesInASegment(numBytesInASegment);
        tieredStoreResultPartition.emitRecord(ByteBuffer.allocate(bufferSize * 5), 1);
        assertThat(bufferPool.bestEffortGetNumOfUsedBuffers()).isEqualTo(5);

        tieredStoreResultPartition.close();
        assertThat(bufferPool.isDestroyed()).isTrue();

        tieredStoreResultPartition.release();

        assertThat(totalBuffers).isEqualTo(globalPool.getNumberOfAvailableMemorySegments());
    }

    @Test
    void testCreateSubpartitionViewAfterRelease() throws Exception {
        final int numBuffers = 10;
        BufferPool bufferPool = globalPool.createBufferPool(numBuffers, numBuffers);
        TieredStoreConfiguration configuration =
                TieredStoreConfiguration.builder(2, 0)
                        .setTieredStoreTiers("DFS")
                        .setTieredStoreSpillingType("FULL")
                        .setBaseDfsHomePath(getTempStorePathDir())
                        .build();
        TieredStoreResultPartition tieredStoreResultPartition =
                createTieredStoreResultPartitionOnlyDfs(2, bufferPool, false, configuration);
        tieredStoreResultPartition.release();
        assertThatThrownBy(
                        () ->
                                tieredStoreResultPartition.createSubpartitionView(
                                        0, new NoOpBufferAvailablityListener()))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testAvailability() throws Exception {
        final int numBuffers = 1;
        BufferPool bufferPool = globalPool.createBufferPool(numBuffers, numBuffers);
        TieredStoreConfiguration configuration =
                TieredStoreConfiguration.builder(2, 0)
                        .setTieredStoreTiers("DFS")
                        .setTieredStoreSpillingType("FULL")
                        .setBaseDfsHomePath(getTempStorePathDir())
                        .build();
        TieredStoreResultPartition tieredStoreResultPartition =
                createTieredStoreResultPartitionOnlyDfs(2, bufferPool, false, configuration);

        tieredStoreResultPartition.emitRecord(ByteBuffer.allocate(bufferSize), 0);
        assertThat(tieredStoreResultPartition.isAvailable()).isFalse();

        // release partition to recycle buffer.
        tieredStoreResultPartition.close();
        tieredStoreResultPartition.release();

        assertThat(tieredStoreResultPartition.isAvailable()).isTrue();
    }

    @Test
    void testMetricsUpdate() throws Exception {
        BufferPool bufferPool = globalPool.createBufferPool(3, 3);
        TieredStoreConfiguration configuration =
                TieredStoreConfiguration.builder(2, 0)
                        .setTieredStoreTiers("DFS")
                        .setTieredStoreSpillingType("FULL")
                        .setBaseDfsHomePath(getTempStorePathDir())
                        .build();
        try (TieredStoreResultPartition partition =
                createTieredStoreResultPartitionOnlyDfs(2, bufferPool, false, configuration); ) {
            partition.emitRecord(ByteBuffer.allocate(bufferSize), 0);
            partition.broadcastRecord(ByteBuffer.allocate(bufferSize));
            assertThat(taskIOMetricGroup.getNumBuffersOutCounter().getCount()).isEqualTo(3);
            assertThat(taskIOMetricGroup.getNumBytesOutCounter().getCount())
                    .isEqualTo(3 * bufferSize);
        }
    }

    @Test
    void testMetricsUpdateForBroadcastOnlyResultPartition() throws Exception {
        BufferPool bufferPool = globalPool.createBufferPool(3, 3);
        TieredStoreConfiguration configuration =
                TieredStoreConfiguration.builder(2, 0)
                        .setTieredStoreTiers("DFS")
                        .setTieredStoreSpillingType("FULL")
                        .setBaseDfsHomePath(getTempStorePathDir())
                        .build();
        try (TieredStoreResultPartition partition =
                createTieredStoreResultPartitionOnlyDfs(2, bufferPool, true, configuration); ) {
            partition.broadcastRecord(ByteBuffer.allocate(bufferSize));
            assertThat(taskIOMetricGroup.getNumBuffersOutCounter().getCount()).isEqualTo(1);

            assertThat(taskIOMetricGroup.getNumBytesOutCounter().getCount()).isEqualTo(bufferSize);
        }
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

    private long readSegmentInfoFromSubpartitionView(
            Tuple2<ResultSubpartitionView, TestingBufferAvailabilityListener>[] viewAndListeners,
            int expectSegmentInfoNumberOfSubpartition)
            throws Exception {
        AtomicInteger totalSegmentInfoSize = new AtomicInteger(0);
        CheckedThread[] subpartitionViewThreads = new CheckedThread[viewAndListeners.length];
        for (int i = 0; i < viewAndListeners.length; i++) {
            // start thread for each view.
            final int subpartition = i;
            CheckedThread subpartitionViewThread =
                    new CheckedThread() {
                        @Override
                        public void go() throws Exception {
                            AtomicInteger segmentInfoSize = new AtomicInteger(0);
                            ResultSubpartitionView view = viewAndListeners[subpartition].f0;
                            while (true) {
                                ResultSubpartition.BufferAndBacklog bufferAndBacklog =
                                        view.getNextBuffer();
                                if (bufferAndBacklog == null) {
                                    viewAndListeners[subpartition].f1.waitForData();
                                    continue;
                                }
                                Buffer buffer = bufferAndBacklog.buffer();
                                assertThat(buffer.getDataType())
                                        .isEqualTo(Buffer.DataType.SEGMENT_EVENT);
                                try {
                                    buffer.asByteBuf().getInt(0);
                                    buffer.asByteBuf().getInt(4);
                                    buffer.asByteBuf().getInt(8);
                                } catch (Exception e) {
                                    throw new RuntimeException(
                                            "Failed to resolve segment info.", e);
                                }
                                segmentInfoSize.addAndGet(buffer.readableBytes());
                                buffer.recycleBuffer();
                                if (segmentInfoSize.get()
                                        == expectSegmentInfoNumberOfSubpartition * 12) {
                                    break;
                                }
                                if (!buffer.isBuffer()) {
                                    throw new RuntimeException(
                                            "The segment info must not be event.");
                                }
                                if (bufferAndBacklog.getNextDataType() == Buffer.DataType.NONE) {
                                    viewAndListeners[subpartition].f1.waitForData();
                                }
                            }
                            totalSegmentInfoSize.getAndAdd(segmentInfoSize.get());
                        }
                    };
            subpartitionViewThreads[subpartition] = subpartitionViewThread;
            subpartitionViewThread.start();
        }
        for (CheckedThread thread : subpartitionViewThreads) {
            thread.sync();
        }
        return totalSegmentInfoSize.get();
    }

    private String getTempStorePathDir() {
        return tmpFolder.getRoot().getPath();
    }

    private static ByteBuffer generateRandomData(int dataSize, Random random) {
        byte[] dataWritten = new byte[dataSize];
        random.nextBytes(dataWritten);
        return ByteBuffer.wrap(dataWritten);
    }

    private TieredStoreResultPartition createTieredStoreResultPartitionOnlyDfs(
            int numSubpartitions,
            BufferPool bufferPool,
            boolean isBroadcastOnly,
            TieredStoreConfiguration tieredStoreConfiguration)
            throws IOException {
        TieredStoreResultPartition tieredStoreResultPartition =
                new TieredStoreResultPartition(
                        new JobID(),
                        "TieredStoreResultPartitionTest",
                        0,
                        new ResultPartitionID(),
                        ResultPartitionType.TIERED_STORE,
                        numSubpartitions,
                        numSubpartitions,
                        null,
                        null,
                        new ResultPartitionManager(),
                        bufferSize,
                        null,
                        isBroadcastOnly,
                        tieredStoreConfiguration,
                        null,
                        () -> bufferPool,
                        null);
        taskIOMetricGroup =
                UnregisteredMetricGroups.createUnregisteredTaskMetricGroup().getIOMetricGroup();
        tieredStoreResultPartition.setup();
        tieredStoreResultPartition.setMetricGroup(taskIOMetricGroup);
        return tieredStoreResultPartition;
    }

    private Tuple2<ResultSubpartitionView, TestingBufferAvailabilityListener>[]
            createSubpartitionViews(TieredStoreResultPartition partition, int numSubpartitions)
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
