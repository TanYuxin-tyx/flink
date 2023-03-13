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

package org.apache.flink.runtime.io.network.partition.tieredstore.upstream.local.disk;

import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer.DataType;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.TieredStoreTestUtils;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.BufferIndexAndChannel;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.DiskCacheManagerOperation;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.OutputMetrics;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.SubpartitionDiskCacheManager;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.io.network.partition.tieredstore.upstream.TieredStoreTestUtils.createBufferBuilder;
import static org.apache.flink.runtime.io.network.partition.tieredstore.upstream.TieredStoreTestUtils.createTestingOutputMetrics;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SubpartitionDiskCacheManager}. */
class SubpartitionDiskCacheManagerTest {

    private static final int SUBPARTITION_ID = 0;

    private static final int RECORD_SIZE = Long.BYTES;

    private int bufferSize = RECORD_SIZE;

    @Test
    void testAppendDataRequestBuffer() throws Exception {
        CompletableFuture<Void> requestBufferFuture = new CompletableFuture<>();
        DiskCacheManagerOperation diskCacheManagerOperation =
                TestingDiskCacheManagerOperation.builder()
                        .setRequestBufferFromPoolSupplier(
                                () -> {
                                    requestBufferFuture.complete(null);
                                    return createBufferBuilder(bufferSize);
                                })
                        .build();
        SubpartitionDiskCacheManager subpartitionDiskCacheManager =
                createSubpartitionMemoryDataManager(diskCacheManagerOperation);
        subpartitionDiskCacheManager.append(createRecord(0), DataType.DATA_BUFFER, false);
        assertThat(requestBufferFuture).isCompleted();
    }

    @Test
    void testAppendEventNotRequestBuffer() throws Exception {
        CompletableFuture<Void> requestBufferFuture = new CompletableFuture<>();
        DiskCacheManagerOperation diskCacheManagerOperation =
                TestingDiskCacheManagerOperation.builder()
                        .setRequestBufferFromPoolSupplier(
                                () -> {
                                    requestBufferFuture.complete(null);
                                    return null;
                                })
                        .build();
        SubpartitionDiskCacheManager subpartitionDiskCacheManager =
                createSubpartitionMemoryDataManager(diskCacheManagerOperation);
        subpartitionDiskCacheManager.append(createRecord(0), DataType.EVENT_BUFFER, false);
        assertThat(requestBufferFuture).isNotDone();
    }

    @Test
    void testAppendEventFinishCurrentBuffer() throws Exception {
        bufferSize = RECORD_SIZE * 3;
        DiskCacheManagerOperation diskCacheManagerOperation =
                TestingDiskCacheManagerOperation.builder()
                        .setRequestBufferFromPoolSupplier(() -> createBufferBuilder(bufferSize))
                        .build();
        SubpartitionDiskCacheManager subpartitionDiskCacheManager =
                createSubpartitionMemoryDataManager(diskCacheManagerOperation);
        subpartitionDiskCacheManager.append(createRecord(0), DataType.DATA_BUFFER, false);
        subpartitionDiskCacheManager.append(createRecord(1), DataType.DATA_BUFFER, false);
        assertThat(subpartitionDiskCacheManager.getFinishedBufferIndex()).isEqualTo(0);
        subpartitionDiskCacheManager.append(createRecord(2), DataType.EVENT_BUFFER, false);
        assertThat(subpartitionDiskCacheManager.getFinishedBufferIndex()).isEqualTo(2);
    }

    @Test
    void testSpillSubpartitionBuffers() throws Exception {
        TestingDiskCacheManagerOperation memoryDataManagerOperation =
                TestingDiskCacheManagerOperation.builder()
                        .setRequestBufferFromPoolSupplier(() -> createBufferBuilder(RECORD_SIZE))
                        .build();
        SubpartitionDiskCacheManager subpartitionDiskCacheManager =
                createSubpartitionMemoryDataManager(memoryDataManagerOperation);
        final int numBuffers = 3;
        for (int i = 0; i < numBuffers; i++) {
            subpartitionDiskCacheManager.append(createRecord(i), DataType.DATA_BUFFER, false);
        }

        List<BufferIndexAndChannel> toStartSpilling =
                TieredStoreTestUtils.createBufferIndexAndChannelsList(0, 0, 1, 2);
        //List<BufferWithIdentity> buffers =
        //        subpartitionDiskCacheManager.spillSubpartitionBuffers(
        //                new ArrayDeque<>(toStartSpilling));
        //assertThat(toStartSpilling)
        //        .zipSatisfy(
        //                buffers,
        //                (expected, spilled) -> {
        //                    assertThat(expected.getBufferIndex())
        //                            .isEqualTo(spilled.getBufferIndex());
        //                    assertThat(expected.getChannel()).isEqualTo(spilled.getChannelIndex());
        //                });
        //List<Integer> expectedValues = Arrays.asList(0, 1, 2);
        //checkBuffersRefCountAndValue(buffers, Arrays.asList(1, 1, 1), expectedValues);
    }

    @Test
    void testMetricsUpdate() throws Exception {
        final int recordSize = bufferSize / 2;
        TestingDiskCacheManagerOperation memoryDataManagerOperation =
                TestingDiskCacheManagerOperation.builder()
                        .setRequestBufferFromPoolSupplier(() -> createBufferBuilder(bufferSize))
                        .build();

        OutputMetrics metrics = createTestingOutputMetrics();
        SubpartitionDiskCacheManager subpartitionDiskCacheManager =
                createSubpartitionMemoryDataManager(memoryDataManagerOperation);
        subpartitionDiskCacheManager.setOutputMetrics(metrics);

        subpartitionDiskCacheManager.append(
                ByteBuffer.allocate(recordSize), DataType.DATA_BUFFER, false);
        ByteBuffer eventBuffer = EventSerializer.toSerializedEvent(EndOfPartitionEvent.INSTANCE);
        final int eventSize = eventBuffer.remaining();
        subpartitionDiskCacheManager.append(
                EventSerializer.toSerializedEvent(EndOfPartitionEvent.INSTANCE),
                DataType.EVENT_BUFFER,
                false);
        assertThat(metrics.getNumBuffersOut().getCount()).isEqualTo(2);
        assertThat(metrics.getNumBytesOut().getCount()).isEqualTo(recordSize + eventSize);
    }

    //private static void checkBuffersRefCountAndValue(
    //        List<BufferWithIdentity> bufferWithIdentities,
    //        List<Integer> expectedRefCounts,
    //        List<Integer> expectedValues) {
    //    for (int i = 0; i < bufferWithIdentities.size(); i++) {
    //        BufferWithIdentity bufferWithIdentity = bufferWithIdentities.get(i);
    //        Buffer buffer = bufferWithIdentity.getBuffer();
    //        assertThat(buffer.getNioBufferReadable().order(ByteOrder.LITTLE_ENDIAN).getInt())
    //                .isEqualTo(expectedValues.get(i));
    //        assertThat(buffer.refCnt()).isEqualTo(expectedRefCounts.get(i));
    //    }
    //}

    private SubpartitionDiskCacheManager createSubpartitionMemoryDataManager(
            DiskCacheManagerOperation diskCacheManagerOperation) {
        return createSubpartitionMemoryDataManager(diskCacheManagerOperation, null);
    }

    private SubpartitionDiskCacheManager createSubpartitionMemoryDataManager(
            DiskCacheManagerOperation diskCacheManagerOperation,
            @Nullable BufferCompressor bufferCompressor) {
        SubpartitionDiskCacheManager subpartitionDiskCacheManager =
                new SubpartitionDiskCacheManager(
                        SUBPARTITION_ID, bufferSize, bufferCompressor, diskCacheManagerOperation);
        subpartitionDiskCacheManager.setOutputMetrics(createTestingOutputMetrics());
        return subpartitionDiskCacheManager;
    }

    private static ByteBuffer createRecord(long value) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(RECORD_SIZE);
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        byteBuffer.putLong(value);
        byteBuffer.flip();
        return byteBuffer;
    }
}
