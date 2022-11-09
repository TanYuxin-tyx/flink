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

package org.apache.flink.runtime.io.network.partition.store.local.memory;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.Buffer.DataType;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferDecompressor;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.store.TieredStoreTestUtils;
import org.apache.flink.runtime.io.network.partition.store.common.BufferIndexAndChannel;
import org.apache.flink.runtime.io.network.partition.store.common.BufferWithIdentity;
import org.apache.flink.runtime.io.network.partition.store.common.ConsumerId;
import org.apache.flink.runtime.io.network.partition.store.tier.local.file.CacheDataManagerOperation;
import org.apache.flink.runtime.io.network.partition.store.tier.local.file.OutputMetrics;
import org.apache.flink.runtime.io.network.partition.store.tier.local.file.SubpartitionCacheDataManager;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.io.network.partition.store.TieredStoreTestUtils.createBufferBuilder;
import static org.apache.flink.runtime.io.network.partition.store.TieredStoreTestUtils.createTestingOutputMetrics;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SubpartitionCacheDataManager}. */
class SubpartitionCacheDataManagerTest {
    private static final int SUBPARTITION_ID = 0;

    private static final int RECORD_SIZE = Long.BYTES;

    private int bufferSize = RECORD_SIZE;

    @Test
    void testAppendDataRequestBuffer() throws Exception {
        CompletableFuture<Void> requestBufferFuture = new CompletableFuture<>();
        CacheDataManagerOperation cacheDataManagerOperation =
                TestingCacheDataManagerOperation.builder()
                        .setRequestBufferFromPoolSupplier(
                                () -> {
                                    requestBufferFuture.complete(null);
                                    return createBufferBuilder(bufferSize);
                                })
                        .build();
        SubpartitionCacheDataManager subpartitionCacheDataManager =
                createSubpartitionMemoryDataManager(cacheDataManagerOperation);
        subpartitionCacheDataManager.append(createRecord(0), DataType.DATA_BUFFER, false);
        assertThat(requestBufferFuture).isCompleted();
    }

    @Test
    void testAppendEventNotRequestBuffer() throws Exception {
        CompletableFuture<Void> requestBufferFuture = new CompletableFuture<>();
        CacheDataManagerOperation cacheDataManagerOperation =
                TestingCacheDataManagerOperation.builder()
                        .setRequestBufferFromPoolSupplier(
                                () -> {
                                    requestBufferFuture.complete(null);
                                    return null;
                                })
                        .build();
        SubpartitionCacheDataManager subpartitionCacheDataManager =
                createSubpartitionMemoryDataManager(cacheDataManagerOperation);
        subpartitionCacheDataManager.append(createRecord(0), DataType.EVENT_BUFFER, false);
        assertThat(requestBufferFuture).isNotDone();
    }

    @Test
    void testAppendEventFinishCurrentBuffer() throws Exception {
        bufferSize = RECORD_SIZE * 3;
        AtomicInteger finishedBuffers = new AtomicInteger(0);
        CacheDataManagerOperation cacheDataManagerOperation =
                TestingCacheDataManagerOperation.builder()
                        .setRequestBufferFromPoolSupplier(() -> createBufferBuilder(bufferSize))
                        .setOnBufferFinishedRunnable(finishedBuffers::incrementAndGet)
                        .build();
        SubpartitionCacheDataManager subpartitionCacheDataManager =
                createSubpartitionMemoryDataManager(cacheDataManagerOperation);
        subpartitionCacheDataManager.append(createRecord(0), DataType.DATA_BUFFER, false);
        subpartitionCacheDataManager.append(createRecord(1), DataType.DATA_BUFFER, false);
        assertThat(finishedBuffers).hasValue(0);
        subpartitionCacheDataManager.append(createRecord(2), DataType.EVENT_BUFFER, false);
        assertThat(finishedBuffers).hasValue(2);
    }

    // @ParameterizedTest
    // @ValueSource(strings = {"LZ4", "LZO", "ZSTD", "NULL"})
    // @Ignore
    // void testCompressBufferAndConsume(String compressionFactoryName) throws Exception {
    //    final int numDataBuffers = 10;
    //    final int numRecordsPerBuffer = 10;
    //    // write numRecordsPerBuffer long record to one buffer, as a single long is
    //    // incompressible.
    //    bufferSize = RECORD_SIZE * numRecordsPerBuffer;
    //    BufferCompressor bufferCompressor =
    //            compressionFactoryName.equals("NULL")
    //                    ? null
    //                    : new BufferCompressor(bufferSize, compressionFactoryName);
    //    BufferDecompressor bufferDecompressor =
    //            compressionFactoryName.equals("NULL")
    //                    ? null
    //                    : new BufferDecompressor(bufferSize, compressionFactoryName);
    //
    //    List<BufferIndexAndChannel> consumedBufferIndexAndChannel = new ArrayList<>();
    //    TestingCacheDataManagerOperation memoryDataManagerOperation =
    //            TestingCacheDataManagerOperation.builder()
    //                    .setRequestBufferFromPoolSupplier(() -> createBufferBuilder(bufferSize))
    //                    .setOnBufferConsumedConsumer(consumedBufferIndexAndChannel::add)
    //                    .build();
    //    SubpartitionCacheDataManager subpartitionCacheDataManager =
    //            createSubpartitionMemoryDataManager(memoryDataManagerOperation, bufferCompressor);
    //    List<Tuple2<Long, DataType>> expectedRecords = new ArrayList<>();
    //
    //    long recordValue = 0L;
    //    for (int i = 0; i < numDataBuffers; i++) {
    //        for (int j = 0; j < numRecordsPerBuffer; j++) {
    //            subpartitionCacheDataManager.append(
    //                    createRecord(recordValue), DataType.DATA_BUFFER, false);
    //            expectedRecords.add(Tuple2.of(recordValue++, DataType.DATA_BUFFER));
    //        }
    //    }
    //    subpartitionCacheDataManager.append(
    //            createRecord(recordValue), DataType.EVENT_BUFFER, false);
    //    expectedRecords.add(Tuple2.of(recordValue, DataType.EVENT_BUFFER));
    //
    //    SubpartitionConsumerCacheDataManager consumer = null;
    //    ArrayList<Optional<BufferAndBacklog>> bufferAndBacklogOpts = new ArrayList<>();
    //    for (int i = 0; i < numDataBuffers + 1; i++) {
    //        bufferAndBacklogOpts.add(consumer.consumeBuffer(i, new ArrayDeque<>()));
    //    }
    //    checkConsumedBufferAndNextDataType(
    //            numRecordsPerBuffer, bufferDecompressor, expectedRecords, bufferAndBacklogOpts);
    //
    //    List<BufferIndexAndChannel> expectedBufferIndexAndChannel =
    //            TieredStoreTestUtils.createBufferIndexAndChannelsList(
    //                    0, IntStream.range(0, numDataBuffers + 1).toArray());
    //    assertThat(consumedBufferIndexAndChannel)
    //            .zipSatisfy(
    //                    expectedBufferIndexAndChannel,
    //                    (consumed, expected) -> {
    //                        assertThat(consumed.getChannel()).isEqualTo(expected.getChannel());
    //                        assertThat(consumed.getBufferIndex())
    //                                .isEqualTo(expected.getBufferIndex());
    //                    });
    // }

    // @Test
    // void testGetBuffersSatisfyStatus() throws Exception {
    //    TestingCacheDataManagerOperation memoryDataManagerOperation =
    //            TestingCacheDataManagerOperation.builder()
    //                    .setRequestBufferFromPoolSupplier(() -> createBufferBuilder(RECORD_SIZE))
    //                    .build();
    //    SubpartitionCacheDataManager subpartitionCacheDataManager =
    //            createSubpartitionMemoryDataManager(memoryDataManagerOperation);
    //    // SubpartitionConsumerCacheDataManager consumer =
    //    //        subpartitionCacheDataManager.registerNewConsumer(ConsumerId.DEFAULT);
    //    SubpartitionConsumerCacheDataManager consumer = null;
    //    final int numBuffers = 4;
    //    for (int i = 0; i < numBuffers; i++) {
    //        subpartitionCacheDataManager.append(createRecord(i), DataType.DATA_BUFFER, false);
    //    }
    //
    //    // spill buffer 1 and 2
    //    List<BufferIndexAndChannel> toStartSpilling =
    //            TieredStoreTestUtils.createBufferIndexAndChannelsList(0, 1, 2);
    //    CompletableFuture<Void> spilledDoneFuture = new CompletableFuture<>();
    //    subpartitionCacheDataManager.spillSubpartitionBuffers(toStartSpilling, spilledDoneFuture);
    //
    //    // consume buffer 0, 1
    //    consumer.consumeBuffer(0, new ArrayDeque<>());
    //    consumer.consumeBuffer(1, new ArrayDeque<>());
    //
    //    checkBufferIndex(
    //            subpartitionCacheDataManager.getBuffersSatisfyStatus(
    //                    BufferSpillingInfoProvider.SpillStatus.ALL, ALL_ANY),
    //            Arrays.asList(0, 1, 2, 3));
    //    checkBufferIndex(
    //            subpartitionCacheDataManager.getBuffersSatisfyStatus(
    //                    SpillStatus.ALL,
    //                    fromStatusAndConsumerId(ConsumeStatus.CONSUMED, ConsumerId.DEFAULT)),
    //            Arrays.asList(0, 1));
    //    checkBufferIndex(
    //            subpartitionCacheDataManager.getBuffersSatisfyStatus(
    //                    SpillStatus.ALL,
    //                    fromStatusAndConsumerId(ConsumeStatus.NOT_CONSUMED, ConsumerId.DEFAULT)),
    //            Arrays.asList(2, 3));
    //    checkBufferIndex(
    //            subpartitionCacheDataManager.getBuffersSatisfyStatus(SpillStatus.SPILL, ALL_ANY),
    //            Arrays.asList(1, 2));
    //    checkBufferIndex(
    //            subpartitionCacheDataManager.getBuffersSatisfyStatus(
    //                    SpillStatus.NOT_SPILL, ALL_ANY),
    //            Arrays.asList(0, 3));
    //    checkBufferIndex(
    //            subpartitionCacheDataManager.getBuffersSatisfyStatus(
    //                    SpillStatus.SPILL,
    //                    fromStatusAndConsumerId(ConsumeStatus.NOT_CONSUMED, ConsumerId.DEFAULT)),
    //            Collections.singletonList(2));
    //    checkBufferIndex(
    //            subpartitionCacheDataManager.getBuffersSatisfyStatus(
    //                    SpillStatus.SPILL,
    //                    fromStatusAndConsumerId(ConsumeStatus.CONSUMED, ConsumerId.DEFAULT)),
    //            Collections.singletonList(1));
    //    checkBufferIndex(
    //            subpartitionCacheDataManager.getBuffersSatisfyStatus(
    //                    SpillStatus.NOT_SPILL,
    //                    fromStatusAndConsumerId(ConsumeStatus.CONSUMED, ConsumerId.DEFAULT)),
    //            Collections.singletonList(0));
    //    checkBufferIndex(
    //            subpartitionCacheDataManager.getBuffersSatisfyStatus(
    //                    SpillStatus.NOT_SPILL,
    //                    fromStatusAndConsumerId(ConsumeStatus.NOT_CONSUMED, ConsumerId.DEFAULT)),
    //            Collections.singletonList(3));
    // }

    @Test
    void testSpillSubpartitionBuffers() throws Exception {
        CompletableFuture<Void> spilledDoneFuture = new CompletableFuture<>();
        TestingCacheDataManagerOperation memoryDataManagerOperation =
                TestingCacheDataManagerOperation.builder()
                        .setRequestBufferFromPoolSupplier(() -> createBufferBuilder(RECORD_SIZE))
                        .build();
        SubpartitionCacheDataManager subpartitionCacheDataManager =
                createSubpartitionMemoryDataManager(memoryDataManagerOperation);
        final int numBuffers = 3;
        for (int i = 0; i < numBuffers; i++) {
            subpartitionCacheDataManager.append(createRecord(i), DataType.DATA_BUFFER, false);
        }

        List<BufferIndexAndChannel> toStartSpilling =
                TieredStoreTestUtils.createBufferIndexAndChannelsList(0, 0, 1, 2);
        List<BufferWithIdentity> buffers =
                subpartitionCacheDataManager.spillSubpartitionBuffers(
                        toStartSpilling, spilledDoneFuture);
        assertThat(toStartSpilling)
                .zipSatisfy(
                        buffers,
                        (expected, spilled) -> {
                            assertThat(expected.getBufferIndex())
                                    .isEqualTo(spilled.getBufferIndex());
                            assertThat(expected.getChannel()).isEqualTo(spilled.getChannelIndex());
                        });
        List<Integer> expectedValues = Arrays.asList(0, 1, 2);
        checkBuffersRefCountAndValue(buffers, Arrays.asList(2, 2, 2), expectedValues);
        spilledDoneFuture.complete(null);
        checkBuffersRefCountAndValue(buffers, Arrays.asList(1, 1, 1), expectedValues);
    }

    @Test
    void testReleaseAndMarkReadableSubpartitionBuffers() throws Exception {
        int targetChannel = 0;
        List<Integer> readableBufferIndex = new ArrayList<>();
        List<MemorySegment> recycledBuffers = new ArrayList<>();
        TestingCacheDataManagerOperation memoryDataManagerOperation =
                TestingCacheDataManagerOperation.builder()
                        .setRequestBufferFromPoolSupplier(
                                () ->
                                        new BufferBuilder(
                                                MemorySegmentFactory.allocateUnpooledSegment(
                                                        bufferSize),
                                                recycledBuffers::add))
                        .setMarkBufferReadableConsumer(
                                (channel, bufferIndex) -> {
                                    assertThat(channel).isEqualTo(targetChannel);
                                    readableBufferIndex.add(bufferIndex);
                                })
                        .build();
        SubpartitionCacheDataManager subpartitionCacheDataManager =
                createSubpartitionMemoryDataManager(memoryDataManagerOperation);
        // append data
        final int numBuffers = 3;
        for (int i = 0; i < numBuffers; i++) {
            subpartitionCacheDataManager.append(createRecord(i), DataType.DATA_BUFFER, false);
        }
        // spill the last buffer and release all buffers.
        List<BufferIndexAndChannel> toRelease =
                TieredStoreTestUtils.createBufferIndexAndChannelsList(targetChannel, 0, 1, 2);
        CompletableFuture<Void> spilledFuture = new CompletableFuture<>();
        subpartitionCacheDataManager.spillSubpartitionBuffers(
                toRelease.subList(numBuffers - 1, numBuffers), spilledFuture);
        subpartitionCacheDataManager.releaseSubpartitionBuffers(toRelease);
        assertThat(readableBufferIndex).isEmpty();
        // not start spilling buffers should be recycled after release.
        checkMemorySegmentValue(recycledBuffers, Arrays.asList(0, 1));

        // after spill finished, need mark readable buffers should trigger notify.
        spilledFuture.complete(null);
        assertThat(readableBufferIndex).containsExactly(2);
        checkMemorySegmentValue(recycledBuffers, Arrays.asList(0, 1, 2));
    }

    @Test
    void testMetricsUpdate() throws Exception {
        final int recordSize = bufferSize / 2;
        TestingCacheDataManagerOperation memoryDataManagerOperation =
                TestingCacheDataManagerOperation.builder()
                        .setRequestBufferFromPoolSupplier(() -> createBufferBuilder(bufferSize))
                        .build();

        OutputMetrics metrics = createTestingOutputMetrics();
        SubpartitionCacheDataManager subpartitionCacheDataManager =
                createSubpartitionMemoryDataManager(memoryDataManagerOperation);
        subpartitionCacheDataManager.setOutputMetrics(metrics);

        subpartitionCacheDataManager.append(
                ByteBuffer.allocate(recordSize), DataType.DATA_BUFFER, false);
        ByteBuffer eventBuffer = EventSerializer.toSerializedEvent(EndOfPartitionEvent.INSTANCE);
        final int eventSize = eventBuffer.remaining();
        subpartitionCacheDataManager.append(
                EventSerializer.toSerializedEvent(EndOfPartitionEvent.INSTANCE),
                DataType.EVENT_BUFFER,
                false);
        assertThat(metrics.getNumBuffersOut().getCount()).isEqualTo(2);
        assertThat(metrics.getNumBytesOut().getCount()).isEqualTo(recordSize + eventSize);
    }

    @Test
    void testConsumerRegisterRepeatedly() {
        TestingCacheDataManagerOperation memoryDataManagerOperation =
                TestingCacheDataManagerOperation.builder().build();
        SubpartitionCacheDataManager subpartitionCacheDataManager =
                createSubpartitionMemoryDataManager(memoryDataManagerOperation);

        ConsumerId consumerId = ConsumerId.newId(null);
        // subpartitionCacheDataManager.registerNewConsumer(consumerId);
        // assertThatThrownBy(() -> subpartitionCacheDataManager.registerNewConsumer(consumerId))
        //        .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testRegisterAndReleaseConsumer() {
        TestingCacheDataManagerOperation memoryDataManagerOperation =
                TestingCacheDataManagerOperation.builder().build();
        SubpartitionCacheDataManager subpartitionCacheDataManager =
                createSubpartitionMemoryDataManager(memoryDataManagerOperation);

        ConsumerId consumerId = ConsumerId.newId(null);
        // subpartitionCacheDataManager.registerNewConsumer(consumerId);
        // subpartitionCacheDataManager.releaseConsumer(consumerId);
        // assertThatNoException()
        //        .isThrownBy(() -> subpartitionCacheDataManager.registerNewConsumer(consumerId));
    }

    private static void checkBufferIndex(
            Deque<BufferIndexAndChannel> bufferWithIdentities, List<Integer> expectedIndexes) {
        List<Integer> bufferIndexes =
                bufferWithIdentities.stream()
                        .map(BufferIndexAndChannel::getBufferIndex)
                        .collect(Collectors.toList());
        assertThat(bufferIndexes).isEqualTo(expectedIndexes);
    }

    private static void checkMemorySegmentValue(
            List<MemorySegment> memorySegments, List<Integer> expectedValues) {
        for (int i = 0; i < memorySegments.size(); i++) {
            assertThat(memorySegments.get(i).getInt(0)).isEqualTo(expectedValues.get(i));
        }
    }

    private static void checkConsumedBufferAndNextDataType(
            int numRecordsPerBuffer,
            BufferDecompressor bufferDecompressor,
            List<Tuple2<Long, DataType>> expectedRecords,
            List<Optional<BufferAndBacklog>> bufferAndBacklogOpt) {
        for (int i = 0; i < bufferAndBacklogOpt.size(); i++) {
            final int bufferIndex = i;
            assertThat(bufferAndBacklogOpt.get(bufferIndex))
                    .hasValueSatisfying(
                            (bufferAndBacklog -> {
                                Buffer buffer = bufferAndBacklog.buffer();
                                if (buffer.isCompressed()) {
                                    assertThat(bufferDecompressor).isNotNull();
                                    buffer =
                                            bufferDecompressor.decompressToIntermediateBuffer(
                                                    buffer);
                                }
                                ByteBuffer byteBuffer =
                                        buffer.getNioBufferReadable()
                                                .order(ByteOrder.LITTLE_ENDIAN);
                                int recordIndex = bufferIndex * numRecordsPerBuffer;
                                while (byteBuffer.hasRemaining()) {
                                    long value = byteBuffer.getLong();
                                    DataType dataType = buffer.getDataType();
                                    assertThat(value)
                                            .isEqualTo(expectedRecords.get(recordIndex).f0);
                                    assertThat(dataType)
                                            .isEqualTo(expectedRecords.get(recordIndex).f1);
                                    recordIndex++;
                                }

                                if (bufferIndex != bufferAndBacklogOpt.size() - 1) {
                                    assertThat(bufferAndBacklog.getNextDataType())
                                            .isEqualTo(expectedRecords.get(recordIndex).f1);
                                } else {
                                    assertThat(bufferAndBacklog.getNextDataType())
                                            .isEqualTo(DataType.NONE);
                                }
                                buffer.recycleBuffer();
                            }));
        }
    }

    private static void checkBuffersRefCountAndValue(
            List<BufferWithIdentity> bufferWithIdentities,
            List<Integer> expectedRefCounts,
            List<Integer> expectedValues) {
        for (int i = 0; i < bufferWithIdentities.size(); i++) {
            BufferWithIdentity bufferWithIdentity = bufferWithIdentities.get(i);
            Buffer buffer = bufferWithIdentity.getBuffer();
            assertThat(buffer.getNioBufferReadable().order(ByteOrder.LITTLE_ENDIAN).getInt())
                    .isEqualTo(expectedValues.get(i));
            assertThat(buffer.refCnt()).isEqualTo(expectedRefCounts.get(i));
        }
    }

    private SubpartitionCacheDataManager createSubpartitionMemoryDataManager(
            CacheDataManagerOperation cacheDataManagerOperation) {
        return createSubpartitionMemoryDataManager(cacheDataManagerOperation, null);
    }

    private SubpartitionCacheDataManager createSubpartitionMemoryDataManager(
            CacheDataManagerOperation cacheDataManagerOperation,
            @Nullable BufferCompressor bufferCompressor) {
        SubpartitionCacheDataManager subpartitionCacheDataManager =
                new SubpartitionCacheDataManager(
                        SUBPARTITION_ID, bufferSize, bufferCompressor, cacheDataManagerOperation);
        subpartitionCacheDataManager.setOutputMetrics(createTestingOutputMetrics());
        return subpartitionCacheDataManager;
    }

    private static ByteBuffer createRecord(long value) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(RECORD_SIZE);
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        byteBuffer.putLong(value);
        byteBuffer.flip();
        return byteBuffer;
    }
}
