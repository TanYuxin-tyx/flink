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

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.PartitionFileWriter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

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

    //    @ParameterizedTest
    //    @ValueSource(booleans = {false, true})
    //    void testProducerMergePartitionFileWriterSpillSuccessfully(boolean isCompressed)
    //            throws Exception {
    //        partitionFileWriter = createProducerMergePartitionFileWriter();
    //        List<NettyPayload> nettyPayloadList = new ArrayList<>();
    //        nettyPayloadList.addAll(
    //                createBufferContextList(
    //                        isCompressed,
    //                        Arrays.asList(Tuple2.of(0, 0), Tuple2.of(1, 1), Tuple2.of(2, 2))));
    //        nettyPayloadList.addAll(
    //                createBufferContextList(
    //                        isCompressed,
    //                        Arrays.asList(Tuple2.of(4, 0), Tuple2.of(5, 1), Tuple2.of(6, 2))));
    //        CompletableFuture<Void> spillFinishedFuture =
    //                partitionFileWriter.write(
    //                        TieredStorageIdMappingUtils.convertId(new ResultPartitionID()),
    //
    // Collections.singletonList(getSubpartitionSpilledBuffers(nettyPayloadList)));
    //        spillFinishedFuture.get();
    //        checkData(
    //                isCompressed,
    //                Arrays.asList(
    //                        Tuple2.of(0, 0),
    //                        Tuple2.of(1, 1),
    //                        Tuple2.of(2, 2),
    //                        Tuple2.of(4, 0),
    //                        Tuple2.of(5, 1),
    //                        Tuple2.of(6, 2)));
    //    }
    //
    //    @Test
    //    void testProducerMergePartitionFileWriterRelease() throws Exception {
    //        partitionFileWriter = createProducerMergePartitionFileWriter();
    //        List<NettyPayload> nettyPayloadList =
    //                new ArrayList<>(
    //                        createBufferContextList(
    //                                false,
    //                                Arrays.asList(Tuple2.of(0, 0), Tuple2.of(1, 1), Tuple2.of(2,
    // 2))));
    //        CompletableFuture<Void> spillFinishedFuture =
    //                partitionFileWriter.write(
    //                        TieredStorageIdMappingUtils.convertId(new ResultPartitionID()),
    //
    // Collections.singletonList(getSubpartitionSpilledBuffers(nettyPayloadList)));
    //        spillFinishedFuture.get();
    //        partitionFileWriter.release();
    //        checkData(false, Arrays.asList(Tuple2.of(0, 0), Tuple2.of(1, 1), Tuple2.of(2, 2)));
    //        assertThatThrownBy(
    //                        () ->
    //                                partitionFileWriter.write(
    //                                        TieredStorageIdMappingUtils.convertId(
    //                                                new ResultPartitionID()),
    //                                        Collections.singletonList(
    //
    // getSubpartitionSpilledBuffers(nettyPayloadList))))
    //                .isInstanceOf(RejectedExecutionException.class);
    //    }
    //
    //    /**
    //     * create buffer with identity list.
    //     *
    //     * @param dataAndIndexes is the list contains pair of (bufferData, bufferIndex).
    //     */
    //    private static List<NettyPayload> createBufferContextList(
    //            boolean isCompressed, List<Tuple2<Integer, Integer>> dataAndIndexes) {
    //        List<NettyPayload> nettyPayloads = new ArrayList<>();
    //        for (Tuple2<Integer, Integer> dataAndIndex : dataAndIndexes) {
    //            Buffer.DataType dataType =
    //                    dataAndIndex.f1 % 2 == 0
    //                            ? Buffer.DataType.EVENT_BUFFER
    //                            : Buffer.DataType.DATA_BUFFER;
    //
    //            MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(BUFFER_SIZE);
    //            segment.putInt(0, dataAndIndex.f0);
    //            Buffer buffer =
    //                    new NetworkBuffer(
    //                            segment, FreeingBufferRecycler.INSTANCE, dataType, BUFFER_SIZE);
    //            if (isCompressed) {
    //                buffer.setCompressed(true);
    //            }
    //            nettyPayloads.add(NettyPayload.newBuffer(buffer, dataAndIndex.f1, 0));
    //        }
    //        return Collections.unmodifiableList(nettyPayloads);
    //    }
    //
    //    private void checkData(boolean isCompressed, List<Tuple2<Integer, Integer>>
    // dataAndIndexes)
    //            throws Exception {
    //        FileChannel readChannel = FileChannel.open(dataFilePath, StandardOpenOption.READ);
    //        ByteBuffer headerBuf = BufferReaderWriterUtil.allocatedHeaderBuffer();
    //        MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(BUFFER_SIZE);
    //        for (Tuple2<Integer, Integer> dataAndIndex : dataAndIndexes) {
    //            Buffer buffer =
    //                    BufferReaderWriterUtil.readFromByteChannel(
    //                            readChannel, headerBuf, segment, (ignore) -> {});
    //            checkNotNull(buffer);
    //            assertThat(buffer.isCompressed()).isEqualTo(isCompressed);
    //            assertThat(buffer.readableBytes()).isEqualTo(BUFFER_SIZE);
    //            assertThat(buffer.getNioBufferReadable().order(ByteOrder.nativeOrder()).getInt())
    //                    .isEqualTo(dataAndIndex.f0);
    //            assertThat(buffer.getDataType().isEvent()).isEqualTo(dataAndIndex.f1 % 2 == 0);
    //        }
    //    }
    //
    //    private PartitionFileWriter createProducerMergePartitionFileWriter() {
    //        return ProducerMergePartitionFile.createPartitionFileWriter(
    //                dataFilePath, new PartitionFileIndex(NUM_SUBPARTITIONS));
    //    }
    //
    //    private static PartitionFileWriter.SubpartitionSpilledBufferContext
    //            getSubpartitionSpilledBuffers(List<NettyPayload> nettyPayloads) {
    //        return new PartitionFileWriter.SubpartitionSpilledBufferContext(
    //                -1,
    //                Collections.singletonList(
    //                        new PartitionFileWriter.SegmentSpilledBufferContext(
    //                                -1, convertToSpilledBufferContext(nettyPayloads), false)));
    //    }
}
