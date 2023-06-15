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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyPayload;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file.PartitionFileWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file.ProducerMergePartitionFile;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file.PartitionFileIndexImpl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/** Tests for {@link PartitionFileWriter}. */
class PartitionFileWriterTest {

    private static final int BUFFER_SIZE = Integer.BYTES;

    private static final int NUM_SUBPARTITIONS = 1;

    private @TempDir Path tempDir;

    private Path dataFilePath;

    private PartitionFileWriter partitionFileWriter;

    @BeforeEach
    void before() {
        this.dataFilePath = tempDir.resolve(".data");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testProducerMergePartitionFileWriterSpillSuccessfully(boolean isCompressed)
            throws Exception {
        partitionFileWriter = createProducerMergePartitionFileWriter();
        List<NettyPayload> nettyPayloadList = new ArrayList<>();
        nettyPayloadList.addAll(
                createBufferContextList(
                        isCompressed,
                        Arrays.asList(Tuple2.of(0, 0), Tuple2.of(1, 1), Tuple2.of(2, 2))));
        nettyPayloadList.addAll(
                createBufferContextList(
                        isCompressed,
                        Arrays.asList(Tuple2.of(4, 0), Tuple2.of(5, 1), Tuple2.of(6, 2))));
        CompletableFuture<Void> spillFinishedFuture =
                partitionFileWriter.write(
                        Collections.singletonList(
                                new Tuple2<>(-1, new Tuple3<>(-1, nettyPayloadList, false))));
        spillFinishedFuture.get();
        checkData(
                isCompressed,
                Arrays.asList(
                        Tuple2.of(0, 0),
                        Tuple2.of(1, 1),
                        Tuple2.of(2, 2),
                        Tuple2.of(4, 0),
                        Tuple2.of(5, 1),
                        Tuple2.of(6, 2)));
    }

    @Test
    void testProducerMergePartitionFileWriterRelease() throws Exception {
        partitionFileWriter = createProducerMergePartitionFileWriter();
        List<NettyPayload> nettyPayloadList =
                new ArrayList<>(
                        createBufferContextList(
                                false,
                                Arrays.asList(Tuple2.of(0, 0), Tuple2.of(1, 1), Tuple2.of(2, 2))));
        CompletableFuture<Void> spillFinishedFuture =
                partitionFileWriter.write(
                        Collections.singletonList(
                                new Tuple2<>(-1, new Tuple3<>(-1, nettyPayloadList, false))));
        spillFinishedFuture.get();
        partitionFileWriter.release();
        checkData(false, Arrays.asList(Tuple2.of(0, 0), Tuple2.of(1, 1), Tuple2.of(2, 2)));
        assertThatThrownBy(
                        () ->
                                partitionFileWriter.write(
                                        Collections.singletonList(
                                                new Tuple2<>(
                                                        -1,
                                                        new Tuple3<>(
                                                                -1, nettyPayloadList, false)))))
                .isInstanceOf(RejectedExecutionException.class);
    }

    /**
     * create buffer with identity list.
     *
     * @param dataAndIndexes is the list contains pair of (bufferData, bufferIndex).
     */
    private static List<NettyPayload> createBufferContextList(
            boolean isCompressed, List<Tuple2<Integer, Integer>> dataAndIndexes) {
        List<NettyPayload> nettyPayloads = new ArrayList<>();
        for (Tuple2<Integer, Integer> dataAndIndex : dataAndIndexes) {
            Buffer.DataType dataType =
                    dataAndIndex.f1 % 2 == 0
                            ? Buffer.DataType.EVENT_BUFFER
                            : Buffer.DataType.DATA_BUFFER;

            MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(BUFFER_SIZE);
            segment.putInt(0, dataAndIndex.f0);
            Buffer buffer =
                    new NetworkBuffer(
                            segment, FreeingBufferRecycler.INSTANCE, dataType, BUFFER_SIZE);
            if (isCompressed) {
                buffer.setCompressed(true);
            }
            nettyPayloads.add(NettyPayload.newBuffer(buffer, dataAndIndex.f1, 0));
        }
        return Collections.unmodifiableList(nettyPayloads);
    }

    private void checkData(boolean isCompressed, List<Tuple2<Integer, Integer>> dataAndIndexes)
            throws Exception {
        FileChannel readChannel = FileChannel.open(dataFilePath, StandardOpenOption.READ);
        ByteBuffer headerBuf = BufferReaderWriterUtil.allocatedHeaderBuffer();
        MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(BUFFER_SIZE);
        for (Tuple2<Integer, Integer> dataAndIndex : dataAndIndexes) {
            Buffer buffer =
                    BufferReaderWriterUtil.readFromByteChannel(
                            readChannel, headerBuf, segment, (ignore) -> {});
            checkNotNull(buffer);
            assertThat(buffer.isCompressed()).isEqualTo(isCompressed);
            assertThat(buffer.readableBytes()).isEqualTo(BUFFER_SIZE);
            assertThat(buffer.getNioBufferReadable().order(ByteOrder.nativeOrder()).getInt())
                    .isEqualTo(dataAndIndex.f0);
            assertThat(buffer.getDataType().isEvent()).isEqualTo(dataAndIndex.f1 % 2 == 0);
        }
    }

    private PartitionFileWriter createProducerMergePartitionFileWriter() {
        return ProducerMergePartitionFile.createPartitionFileWriter(
                dataFilePath, new PartitionFileIndexImpl(NUM_SUBPARTITIONS));
    }
}
