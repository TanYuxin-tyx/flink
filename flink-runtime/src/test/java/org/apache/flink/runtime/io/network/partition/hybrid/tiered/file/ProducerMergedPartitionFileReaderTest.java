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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.file;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageIdMappingUtils;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.HEADER_LENGTH;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageTestUtils.generateBuffersToWrite;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ProducerMergedPartitionFileReader}. */
class ProducerMergedPartitionFileReaderTest {

    private static final int DEFAULT_NUM_SUBPARTITION = 1;

    private static final int DEFAULT_SEGMENT_NUM = 1;

    private static final int DEFAULT_SEGMENT_ID = 0;

    private static final int DEFAULT_BUFFER_NUMBER = 5;

    private static final int DEFAULT_BUFFER_SIZE = 10;

    private static final String DEFAULT_TEST_FILE_NAME = "testFile";

    private static final String DEFAULT_TEST_INDEX_NAME = "testIndex";

    private static final TieredStoragePartitionId DEFAULT_PARTITION_ID =
            TieredStorageIdMappingUtils.convertId(new ResultPartitionID());

    private static final TieredStorageSubpartitionId DEFAULT_SUBPARTITION_ID =
            new TieredStorageSubpartitionId(0);

    @TempDir private Path tempFolder;

    private Path testFilePath;

    private ProducerMergedPartitionFileReader partitionFileReader;

    @BeforeEach
    void before() throws ExecutionException, InterruptedException {
        Path testIndexPath = new File(tempFolder.toFile(), DEFAULT_TEST_INDEX_NAME).toPath();
        ProducerMergedPartitionFileIndex partitionFileIndex =
                new ProducerMergedPartitionFileIndex(
                        DEFAULT_NUM_SUBPARTITION, testIndexPath, 256, Long.MAX_VALUE);
        testFilePath = new File(tempFolder.toFile(), DEFAULT_TEST_FILE_NAME).toPath();
        ProducerMergedPartitionFileWriter partitionFileWriter =
                new ProducerMergedPartitionFileWriter(testFilePath, partitionFileIndex);
        // Write buffers to disk by writer
        List<PartitionFileWriter.SubpartitionBufferContext> subpartitionBuffers =
                generateBuffersToWrite(
                        DEFAULT_NUM_SUBPARTITION,
                        DEFAULT_SEGMENT_NUM,
                        DEFAULT_BUFFER_NUMBER,
                        DEFAULT_BUFFER_SIZE);
        partitionFileWriter.write("", DEFAULT_PARTITION_ID, subpartitionBuffers).get();
        partitionFileReader =
                new ProducerMergedPartitionFileReader(testFilePath, partitionFileIndex);
    }

    @Test
    void testReadBuffer1() throws IOException {
        PartitionFileReader.PartialBuffer partialBuffer = null;
        for (int bufferIndex = 0; bufferIndex < DEFAULT_BUFFER_NUMBER; ++bufferIndex) {
            List<Buffer> buffers = readBuffer(bufferIndex, DEFAULT_SUBPARTITION_ID, partialBuffer);
            assertThat(buffers).isNotNull();
            for (Buffer buffer : buffers) {
                if (buffer instanceof PartitionFileReader.PartialBuffer) {
                    partialBuffer = (PartitionFileReader.PartialBuffer) buffer;
                } else {
                    buffer.recycleBuffer();
                }
            }
        }
        MemorySegment memorySegment =
                MemorySegmentFactory.allocateUnpooledSegment(DEFAULT_BUFFER_SIZE);
        assertThat(
                        partitionFileReader.readBuffer(
                                false,
                                "",
                                DEFAULT_PARTITION_ID,
                                DEFAULT_SUBPARTITION_ID,
                                DEFAULT_SEGMENT_ID,
                                DEFAULT_BUFFER_NUMBER + 1,
                                memorySegment,
                                FreeingBufferRecycler.INSTANCE,
                                BufferReaderWriterUtil.allocatedHeaderBuffer(),
                                null))
                .isNull();
    }

    @Test
    void testReadBuffer() throws IOException {
        for (int bufferIndex = 0; bufferIndex < DEFAULT_BUFFER_NUMBER; ++bufferIndex) {
            List<Buffer> buffers = readBuffer(bufferIndex, DEFAULT_SUBPARTITION_ID);
            assertThat(buffers).isNotNull();
            buffers.forEach(Buffer::recycleBuffer);
        }
        MemorySegment memorySegment =
                MemorySegmentFactory.allocateUnpooledSegment(DEFAULT_BUFFER_SIZE);
        assertThat(
                        partitionFileReader.readBuffer(
                                false,
                                "",
                                DEFAULT_PARTITION_ID,
                                DEFAULT_SUBPARTITION_ID,
                                DEFAULT_SEGMENT_ID,
                                DEFAULT_BUFFER_NUMBER + 1,
                                memorySegment,
                                FreeingBufferRecycler.INSTANCE,
                                BufferReaderWriterUtil.allocatedHeaderBuffer(),
                                null))
                .isNull();
    }

    @Test
    void testGetPriority() throws IOException {
        AtomicLong currentFileOffset = new AtomicLong(0);
        PartitionFileReader.PartialBuffer partialBuffer = null;
        for (int bufferIndex = 0; bufferIndex < DEFAULT_BUFFER_NUMBER; ) {
            List<Buffer> buffers = readBuffer(bufferIndex, DEFAULT_SUBPARTITION_ID, partialBuffer);
            assertThat(buffers).isNotNull();
            for (Buffer buffer : buffers) {
                if (buffer instanceof PartitionFileReader.PartialBuffer) {
                    partialBuffer = (PartitionFileReader.PartialBuffer) buffer;
                    currentFileOffset.set(partialBuffer.getFileOffset());
                } else {
                    bufferIndex++;
                    currentFileOffset.addAndGet(buffer.readableBytes() + HEADER_LENGTH);
                    buffer.recycleBuffer();
                }
            }
            long expectedFileOffset =
                    bufferIndex < DEFAULT_BUFFER_NUMBER ? currentFileOffset.get() : Long.MAX_VALUE;
            assertThat(
                            partitionFileReader.getPriority(
                                    DEFAULT_PARTITION_ID,
                                    DEFAULT_SUBPARTITION_ID,
                                    DEFAULT_SEGMENT_ID,
                                    bufferIndex,
                                    BufferReaderWriterUtil.allocatedHeaderBuffer()))
                    .isEqualTo(expectedFileOffset);
        }
    }

    @Test
    void testGetPriorityWhenBufferIsNotEnough() throws IOException {
        AtomicLong currentFileOffset = new AtomicLong(0);
        PartitionFileReader.PartialBuffer partialBuffer = null;
        int bufferIndex = 0;
        for (; bufferIndex < DEFAULT_BUFFER_NUMBER; ) {
            List<Buffer> buffers = readBuffer(bufferIndex, DEFAULT_SUBPARTITION_ID, partialBuffer);
            assertThat(buffers).isNotNull();
            for (Buffer buffer : buffers) {
                if (buffer instanceof PartitionFileReader.PartialBuffer) {
                    partialBuffer = (PartitionFileReader.PartialBuffer) buffer;
                } else {
                    bufferIndex++;
                    currentFileOffset.addAndGet(buffer.readableBytes() + HEADER_LENGTH);
                    buffer.recycleBuffer();
                }
            }
        }
        assertThat(partialBuffer).isNotNull();
        partialBuffer.recycleBuffer();
        assertThat(
                        partitionFileReader.getPriority(
                                DEFAULT_PARTITION_ID,
                                DEFAULT_SUBPARTITION_ID,
                                DEFAULT_SEGMENT_ID,
                                DEFAULT_BUFFER_NUMBER,
                                BufferReaderWriterUtil.allocatedHeaderBuffer()))
                .isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void testCacheExceedMaxNumber() throws IOException {
        int cacheNumber = 3;
        AtomicInteger indexQueryTime = new AtomicInteger(0);
        TestingProducerMergedPartitionFileIndex partitionFileIndex =
                new TestingProducerMergedPartitionFileIndex.Builder()
                        .setIndexFilePath(new File(tempFolder.toFile(), "test-Index").toPath())
                        .setGetRegionFunction(
                                (subpartitionId, integer) -> {
                                    indexQueryTime.incrementAndGet();
                                    return Optional.of(
                                            new ProducerMergedPartitionFileIndex.FixedSizeRegion(
                                                    0, 0, 90, 2));
                                })
                        .build();
        partitionFileReader =
                new ProducerMergedPartitionFileReader(
                        testFilePath, partitionFileIndex, cacheNumber);
        // Read different subpartitions from the reader and make cache reach max number.
        for (int subpartitionId = 0; subpartitionId < cacheNumber * 2; ++subpartitionId) {
            assertThat(
                            readBuffer(
                                    subpartitionId < cacheNumber ? 0 : 1,
                                    new TieredStorageSubpartitionId(subpartitionId % cacheNumber)))
                    .isNotNull();
        }
        // The following buffer reading from other subpartitions can only query the index.
        assertThat(readBuffer(0, new TieredStorageSubpartitionId(0))).isNotNull();
        assertThat(readBuffer(0, new TieredStorageSubpartitionId(1))).isNotNull();
        assertThat(indexQueryTime).hasValue(6);
    }

    @Test
    void testRelease() {
        assertThat(testFilePath.toFile().exists()).isTrue();
        partitionFileReader.release();
        assertThat(testFilePath.toFile().exists()).isFalse();
    }

    private List<Buffer> readBuffer(int bufferIndex, TieredStorageSubpartitionId subpartitionId)
            throws IOException {
        return readBuffer(bufferIndex, subpartitionId, null);
    }

    private List<Buffer> readBuffer(
            int bufferIndex,
            TieredStorageSubpartitionId subpartitionId,
            PartitionFileReader.PartialBuffer partialBuffer)
            throws IOException {
        MemorySegment memorySegment =
                MemorySegmentFactory.allocateUnpooledSegment(DEFAULT_BUFFER_SIZE);
        return partitionFileReader.readBuffer(
                false,
                "",
                DEFAULT_PARTITION_ID,
                subpartitionId,
                DEFAULT_SEGMENT_ID,
                bufferIndex,
                memorySegment,
                FreeingBufferRecycler.INSTANCE,
                BufferReaderWriterUtil.allocatedHeaderBuffer(),
                partialBuffer);
    }
}
