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
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferHeader;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.CompositeBuffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;

/** {@link PartitionFileReader} defines the read logic for different types of shuffle files. */
public interface PartitionFileReader {

    /**
     * Read a buffer from the partition file.
     *
     * @param partitionId the partition id of the buffer
     * @param subpartitionId the subpartition id of the buffer
     * @param segmentId the segment id of the buffer
     * @param bufferIndex the index of buffer
     * @param memorySegment the empty buffer to store the read buffer
     * @param recycler the buffer recycler
     * @param partialBuffer the previous partial buffer. The partial buffer is not null only when
     *     the last read has a partial buffer, it will construct a full buffer during the read
     *     process.
     * @return an empty list if there is no data otherwise return the read buffers.
     */
    List<Buffer> readBuffer(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId,
            int bufferIndex,
            MemorySegment memorySegment,
            BufferRecycler recycler,
            @Nullable PartialBuffer partialBuffer)
            throws IOException;

    /**
     * Get the priority for reading a particular buffer from the partitioned file. The priority is
     * defined as, it is suggested to read buffers with higher priority (smaller value) in prior to
     * buffers with lower priority (larger value).
     *
     * <p>Depending on the partition file implementation, following the suggestions should typically
     * result in better performance and efficiency. This can be achieved by e.g. choosing preloaded
     * data over others, optimizing the order of disk access to be more sequential, etc.
     *
     * <p>Note: Priorities are suggestions rather than a requirements. The caller can still read
     * data in whichever order it wants.
     *
     * @param partitionId the partition id of the buffer
     * @param subpartitionId the subpartition id of the buffer
     * @param segmentId the segment id of the buffer
     * @param bufferIndex the index of buffer
     * @return the priority of the {@link PartitionFileReader}.
     */
    long getPriority(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId,
            int bufferIndex);

    /** Release the {@link PartitionFileReader}. */
    void release();

    /** A {@link PartialBuffer} is a part slice of a larger buffer. */
    class PartialBuffer implements Buffer {

        private final long fileOffset;

        private final CompositeBuffer compositeBuffer;

        private final BufferHeader bufferHeader;

        public PartialBuffer(
                long fileOffset, CompositeBuffer compositeBuffer, BufferHeader bufferHeader) {
            checkArgument(fileOffset >= 0);
            this.fileOffset = fileOffset;
            this.compositeBuffer = compositeBuffer;
            this.bufferHeader = bufferHeader;
        }

        /**
         * Returns the underlying file offset. Note that the file offset includes the length of the
         * partial buffer.
         */
        public long getFileOffset() {
            return fileOffset;
        }

        public CompositeBuffer getCompositeBuffer() {
            return compositeBuffer;
        }

        public BufferHeader getBufferHeader() {
            return bufferHeader;
        }

        @Override
        public boolean isBuffer() {
            return compositeBuffer.isBuffer();
        }

        @Override
        public MemorySegment getMemorySegment() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getMemorySegmentOffset() {
            throw new UnsupportedOperationException();
        }

        @Override
        public BufferRecycler getRecycler() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setRecycler(BufferRecycler bufferRecycler) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void recycleBuffer() {
            if (compositeBuffer != null) {
                compositeBuffer.recycleBuffer();
            }
        }

        @Override
        public boolean isRecycled() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Buffer retainBuffer() {
            return compositeBuffer.retainBuffer();
        }

        @Override
        public Buffer readOnlySlice() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Buffer readOnlySlice(int index, int length) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getMaxCapacity() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getReaderIndex() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setReaderIndex(int readerIndex) throws IndexOutOfBoundsException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getSize() {
            return compositeBuffer == null ? 0 : compositeBuffer.getSize();
        }

        @Override
        public void setSize(int writerIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int readableBytes() {
            return compositeBuffer == null ? 0 : compositeBuffer.readableBytes();
        }

        @Override
        public ByteBuffer getNioBufferReadable() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ByteBuffer getNioBuffer(int index, int length) throws IndexOutOfBoundsException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setAllocator(ByteBufAllocator allocator) {
            compositeBuffer.setAllocator(allocator);
        }

        @Override
        public ByteBuf asByteBuf() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isCompressed() {
            return compositeBuffer.isCompressed();
        }

        @Override
        public void setCompressed(boolean isCompressed) {
            throw new UnsupportedOperationException();
        }

        @Override
        public DataType getDataType() {
            return compositeBuffer.getDataType();
        }

        @Override
        public void setDataType(DataType dataType) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int refCnt() {
            throw new UnsupportedOperationException();
        }
    }
}
