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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferHeader;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.CompositeBuffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.HEADER_LENGTH;
import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.positionToNextBuffer;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The implementation of {@link PartitionFileReader} with producer-merge mode. In this mode, the
 * shuffle data is written in the producer side, the consumer side need to read multiple producers
 * to get its partition data.
 *
 * <p>Note that one partition file may contain the data of multiple subpartitions.
 */
public class ProducerMergedPartitionFileReader implements PartitionFileReader {

    private static final Logger LOG =
            LoggerFactory.getLogger(ProducerMergedPartitionFileReader.class);

    public static final int SHOULD_READ_REGION_FROM_INDEX = -1;

    private final Path dataFilePath;

    private final ProducerMergedPartitionFileIndex dataIndex;

    private volatile FileChannel fileChannel;

    ProducerMergedPartitionFileReader(
            Path dataFilePath, ProducerMergedPartitionFileIndex dataIndex) {
        this.dataFilePath = dataFilePath;
        this.dataIndex = dataIndex;
    }

    @Override
    public List<Buffer> readBuffer(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId,
            int bufferIndex,
            MemorySegment memorySegment,
            BufferRecycler recycler,
            ByteBuffer reusedHeaderBuffer,
            @Nullable PartialBuffer partialBuffer)
            throws IOException {

        lazyInitializeFileChannel();

        // Get the read offset, including the start offset, the end offset
        Tuple2<Long, Long> readStartEndOffset =
                getReadStartEndOffset(
                        subpartitionId, bufferIndex, partialBuffer, reusedHeaderBuffer);
        if (readStartEndOffset == null) {
            return null;
        }
        long readStartOffset = readStartEndOffset.f0;
        long readEndOffset = readStartEndOffset.f1;
        checkState(readStartOffset <= readEndOffset);

        // Calculate the number of bytes to read
        int numBytesToRead =
                Math.min(memorySegment.size(), (int) (readEndOffset - readStartOffset));
        if (numBytesToRead == 0) {
            return null;
        }
        ByteBuffer byteBuffer = memorySegment.wrap(0, numBytesToRead);
        fileChannel.position(readStartOffset);

        // Read data to the memory segment, note the read size is numBytesToRead
        readFileDataToBuffer(memorySegment, recycler, byteBuffer);

        NetworkBuffer buffer = new NetworkBuffer(memorySegment, recycler);
        buffer.setSize(byteBuffer.remaining());
        List<Buffer> readBuffers = new LinkedList<>();
        boolean noMoreDataInRegion = false;
        try {
            // Slice the read memory segment to multiple small network buffers and add them to
            // readBuffers
            Tuple2<CompositeBuffer, BufferHeader> partial =
                    sliceBuffer(byteBuffer, buffer, reusedHeaderBuffer, partialBuffer, readBuffers);
            if (readStartOffset + numBytesToRead < readEndOffset) {
                // If the region is not finished read, generate a partial buffer to store the
                // partial data, then append the partial buffer to the tail of readBuffers
                partialBuffer =
                        new PartialBuffer(
                                readStartOffset + numBytesToRead,
                                readEndOffset,
                                partial.f0,
                                partial.f1);
                readBuffers.add(partialBuffer);
            } else {
                // The region is read completely
                checkState(partial.f0 == null);
                noMoreDataInRegion = true;
            }
        } catch (Throwable throwable) {
            LOG.error("Failed to split the read buffer {}.", byteBuffer, throwable);
            throw throwable;
        } finally {
            buffer.recycleBuffer();
        }

        checkState(
                noMoreDataInRegion && readStartOffset + numBytesToRead == readEndOffset
                        || readStartOffset + numBytesToRead < readEndOffset);
        return readBuffers;
    }

    @Override
    public long getPriority(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId,
            int bufferIndex,
            ByteBuffer reusedHeaderBuffer) {
        lazyInitializeFileChannel();
        Optional<ProducerMergedPartitionFileIndex.FixedSizeRegion> readRegion =
                dataIndex.getRegion(subpartitionId, bufferIndex);
        if (!readRegion.isPresent()) {
            return Long.MAX_VALUE;
        }
        ProducerMergedPartitionFileIndex.FixedSizeRegion region = readRegion.get();
        moveFileOffsetToBuffer(region, bufferIndex, reusedHeaderBuffer);
        return region.getRegionFileOffset();
    }

    @Override
    public void release() {
        if (fileChannel != null) {
            try {
                fileChannel.close();
            } catch (IOException e) {
                ExceptionUtils.rethrow(e, "Failed to close file channel.");
            }
        }
        IOUtils.deleteFileQuietly(dataFilePath);
    }

    /**
     * Initialize the file channel in a lazy manner, which can reduce usage of the file descriptor
     * resource.
     */
    private void lazyInitializeFileChannel() {
        if (fileChannel == null) {
            try {
                fileChannel = FileChannel.open(dataFilePath, StandardOpenOption.READ);
            } catch (IOException e) {
                ExceptionUtils.rethrow(e, "Failed to open file channel.");
            }
        }
    }

    /**
     * Slice the read memory segment to multiple small network buffers.
     *
     * <p>Note that although the method appears to be split into multiple buffers, the sliced
     * buffers still share the same one actual underlying memory segment.
     *
     * @param byteBuffer the byte buffer to be sliced
     * @param buffer the network buffer actually shares the same memory segment with byteBuffer.
     *     This argument is only to call the method NetworkBuffer#readOnlySlice to read a slice of a
     *     memory segment
     * @param reusedHeaderBuffer the reused header buffer, it may contain the partial header buffer
     *     from the previous read
     * @param partialBuffer the partial buffer, if the partial buffer is not null, it contains the
     *     partial data buffer from the previous read
     * @param readBuffers the read buffers list is to accept the sliced buffers
     * @return the first field is the partial data buffer, the second field is the partial buffer's
     *     header, indicating the actual length of the partial buffer
     */
    private Tuple2<CompositeBuffer, BufferHeader> sliceBuffer(
            ByteBuffer byteBuffer,
            NetworkBuffer buffer,
            ByteBuffer reusedHeaderBuffer,
            @Nullable PartialBuffer partialBuffer,
            List<Buffer> readBuffers) {
        BufferHeader header = partialBuffer == null ? null : partialBuffer.getBufferHeader();
        CompositeBuffer slicedBuffer =
                partialBuffer == null ? null : partialBuffer.getCompositeBuffer();
        if (header == null) {
            checkState(slicedBuffer == null || reusedHeaderBuffer.position() > 0);
        }
        checkState(slicedBuffer == null || slicedBuffer.missingLength() > 0);

        while (byteBuffer.hasRemaining()) {
            // Parse the small buffer's header
            if (header == null
                    && (header = parseBufferHeader(byteBuffer, reusedHeaderBuffer)) == null) {
                break;
            }

            // If the previous partial buffer is not exist
            if (slicedBuffer == null) {
                if (header.getLength() <= byteBuffer.remaining()) {
                    // The remaining data length in the buffer is enough to generate a new small
                    // sliced network buffer. The small sliced buffer is not a partial buffer, we
                    // should read the slice of the buffer directly
                    buffer.retainBuffer();
                    slicedBuffer = new CompositeBuffer(header);
                    slicedBuffer.addPartialBuffer(
                            buffer.readOnlySlice(byteBuffer.position(), header.getLength()));
                    byteBuffer.position(byteBuffer.position() + header.getLength());
                } else {
                    // The remaining data length in the buffer is smaller than the actual length of
                    // the buffer, so we should generate a new partial buffer, allowing for
                    // generating a new complete buffer during the next read operation
                    if (byteBuffer.hasRemaining()) {
                        buffer.retainBuffer();
                        slicedBuffer = new CompositeBuffer(header);
                        slicedBuffer.addPartialBuffer(
                                buffer.readOnlySlice(
                                        byteBuffer.position(), byteBuffer.remaining()));
                    }
                    break;
                }
            } else {
                // If there is a previous small partial buffer, the current read operation should
                // read additional data and combine it with the existing partial to construct a new
                // complete buffer
                buffer.retainBuffer();
                int position = byteBuffer.position() + slicedBuffer.missingLength();
                slicedBuffer.addPartialBuffer(
                        buffer.readOnlySlice(byteBuffer.position(), slicedBuffer.missingLength()));
                byteBuffer.position(position);
            }

            header = null;
            readBuffers.add(slicedBuffer);
            slicedBuffer = null;
        }
        checkState(slicedBuffer == null || slicedBuffer.missingLength() > 0);
        if (header != null) {
            reusedHeaderBuffer.clear();
        }
        return Tuple2.of(slicedBuffer, header);
    }

    private Tuple2<Long, Long> getReadStartEndOffset(
            TieredStorageSubpartitionId subpartitionId,
            int bufferIndex,
            PartialBuffer partialBuffer,
            ByteBuffer reusedHeaderBuffer)
            throws IOException {
        long readEndOffset;
        long readStartOffset;
        if (partialBuffer == null
                || partialBuffer.getReadEndOffset() == SHOULD_READ_REGION_FROM_INDEX) {
            checkState(
                    partialBuffer == null
                            || partialBuffer.getCompositeBuffer() == null
                                    && partialBuffer.getBufferHeader() == null);
            Optional<ProducerMergedPartitionFileIndex.FixedSizeRegion> region =
                    dataIndex.getRegion(subpartitionId, bufferIndex);
            if (!region.isPresent()) {
                return null;
            } else {
                moveFileOffsetToBuffer(region.get(), bufferIndex, reusedHeaderBuffer);
                readStartOffset = fileChannel.position();
                readEndOffset = region.get().getRegionFileEndOffset();
            }
        } else {
            readStartOffset = partialBuffer.getReadStartOffset();
            readEndOffset = partialBuffer.getReadEndOffset();
        }
        return Tuple2.of(readStartOffset, readEndOffset);
    }

    private void readFileDataToBuffer(
            MemorySegment memorySegment, BufferRecycler recycler, ByteBuffer byteBuffer)
            throws IOException {
        try {
            BufferReaderWriterUtil.readByteBufferFully(fileChannel, byteBuffer);
            byteBuffer.flip();
        } catch (Throwable throwable) {
            recycler.recycle(memorySegment);
            throw throwable;
        }
    }

    private BufferHeader parseBufferHeader(ByteBuffer buffer, ByteBuffer reusedHeaderBuffer) {
        BufferHeader header = null;
        try {
            if (reusedHeaderBuffer.position() > 0) {
                // The buffer contains a partial header from the previous read. The current read
                // operation should read additional data and combine it with the existing partial
                // data to construct a new complete header.
                checkState(reusedHeaderBuffer.position() < HEADER_LENGTH);
                while (reusedHeaderBuffer.hasRemaining()) {
                    reusedHeaderBuffer.put(buffer.get());
                }
                reusedHeaderBuffer.flip();
                header = BufferReaderWriterUtil.parseBufferHeader(reusedHeaderBuffer);
                reusedHeaderBuffer.clear();
            }

            if (header == null && buffer.remaining() < HEADER_LENGTH) {
                // The remaining data length in the buffer is smaller than the header. Therefore,
                // the remaining data is stored in the reusedHeaderBuffer. This will allow for
                // constructing a complete reader during the next read operation.
                reusedHeaderBuffer.put(buffer);
            } else if (header == null) {
                // The remaining data length in the buffer is enough to construct a new complete
                // buffer
                header = BufferReaderWriterUtil.parseBufferHeader(buffer);
                reusedHeaderBuffer.clear();
            }
        } catch (Throwable throwable) {
            reusedHeaderBuffer.clear();
            LOG.error("Failed to parse buffer header.", throwable);
            throw throwable;
        }

        return header;
    }

    /**
     * Relocates the file channel offset to the position of the specified buffer index.
     *
     * @param bufferIndex denotes the index of the buffer.
     */
    private void moveFileOffsetToBuffer(
            ProducerMergedPartitionFileIndex.FixedSizeRegion region,
            int bufferIndex,
            ByteBuffer reusedHeaderBuffer) {
        try {
            checkNotNull(fileChannel).position(region.getRegionFileOffset());
            for (int i = 0; i < (bufferIndex - region.getFirstBufferIndex()); ++i) {
                positionToNextBuffer(fileChannel, reusedHeaderBuffer);
            }
            reusedHeaderBuffer.clear();
        } catch (IOException e) {
            ExceptionUtils.rethrow(e, "Failed to move file offset");
        }
    }
}
