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

import org.apache.flink.annotation.VisibleForTesting;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.HEADER_LENGTH;
import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.positionToNextBuffer;
import static org.apache.flink.util.Preconditions.checkArgument;
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

    /**
     * Max number of caches.
     *
     * <p>The constant defines the maximum number of caches that can be created. Its value is set to
     * 10000, which is considered sufficient for most parallel jobs. Each cache only contains
     * references and numerical variables and occupies a minimal amount of memory so the value is
     * not excessively large.
     */
    private static final int DEFAULT_MAX_CACHE_NUM = 10000;

    /**
     * Buffer offset caches stored in map.
     *
     * <p>The key is the combination of {@link TieredStorageSubpartitionId} and buffer index. The
     * value is the buffer offset cache, which includes file offset of the buffer index, the region
     * containing the buffer index and next buffer index to consume.
     */
    private final Map<Tuple2<TieredStorageSubpartitionId, Integer>, BufferOffsetCache>
            bufferOffsetCaches;

    private final ByteBuffer reusedHeaderBuffer = BufferReaderWriterUtil.allocatedHeaderBuffer();

    private final Path dataFilePath;

    private final ProducerMergedPartitionFileIndex dataIndex;

    private final int maxCacheNumber;

    private volatile FileChannel fileChannel;

    /** The current number of caches. */
    private int numCaches;

    ProducerMergedPartitionFileReader(
            Path dataFilePath, ProducerMergedPartitionFileIndex dataIndex) {
        this(dataFilePath, dataIndex, DEFAULT_MAX_CACHE_NUM);
    }

    @VisibleForTesting
    ProducerMergedPartitionFileReader(
            Path dataFilePath, ProducerMergedPartitionFileIndex dataIndex, int maxCacheNumber) {
        this.dataFilePath = dataFilePath;
        this.dataIndex = dataIndex;
        this.bufferOffsetCaches = new HashMap<>();
        this.maxCacheNumber = maxCacheNumber;
    }

    @Override
    public Tuple2<List<Buffer>, Boolean> readBuffer(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId,
            int bufferIndex,
            MemorySegment memorySegment,
            BufferRecycler recycler,
            @Nullable PartialBuffer partialBuffer)
            throws IOException {

        lazyInitializeFileChannel();

        List<Buffer> readBuffers = new LinkedList<>();
        Tuple2<TieredStorageSubpartitionId, Integer> cacheKey =
                Tuple2.of(subpartitionId, bufferIndex);
        Optional<BufferOffsetCache> cache = tryGetCache(cacheKey, true);
        if (!cache.isPresent()) {
            return Tuple2.of(Collections.emptyList(), false);
        }

        // Get the read offset, including the start offset, the end offset
        int numPartialReadBytes =
                (partialBuffer == null) ? 0 : partialBuffer.readableBytes() + HEADER_LENGTH;
        long readStartOffset = cache.get().getFileOffset() + numPartialReadBytes;
        long readEndOffset = cache.get().region.getRegionFileEndOffset();
        int numBytesToRead =
                Math.min(memorySegment.size(), (int) (readEndOffset - readStartOffset));
        checkState(readStartOffset <= readEndOffset);

        if (numBytesToRead == 0) {
            return Tuple2.of(Collections.emptyList(), false);
        }

        ByteBuffer byteBuffer = memorySegment.wrap(0, numBytesToRead);
        fileChannel.position(readStartOffset);

        // Read data to the memory segment, note the read size is numBytesToRead
        readFileDataToBuffer(memorySegment, recycler, byteBuffer);

        NetworkBuffer buffer = new NetworkBuffer(memorySegment, recycler);
        buffer.setSize(byteBuffer.remaining());

        int numBytesRealRead;
        boolean shouldContinueRead;
        try {
            // Slice the read memory segment to multiple small network buffers and add them to
            // readBuffers
            Tuple2<PartialBuffer, Integer> partialAndNumReadBytes =
                    sliceBuffer(byteBuffer, buffer, partialBuffer, readBuffers);
            partialBuffer = partialAndNumReadBytes.f0;
            numBytesRealRead = partialAndNumReadBytes.f1;
            checkState(
                    numBytesRealRead <= numBytesToRead
                            && numBytesToRead - numBytesRealRead < HEADER_LENGTH);

            if (readStartOffset + numBytesRealRead < readEndOffset) {
                shouldContinueRead = true;
            } else {
                // The region is read completely
                checkState(partialBuffer == null);
                shouldContinueRead = false;
            }
        } catch (Throwable throwable) {
            LOG.error("Failed to split the read buffer {}.", byteBuffer, throwable);
            throw throwable;
        } finally {
            buffer.recycleBuffer();
        }

        updateBufferOffsetCache(
                subpartitionId,
                bufferIndex,
                cache.get(),
                readBuffers,
                partialBuffer,
                readStartOffset,
                readEndOffset,
                numBytesRealRead,
                shouldContinueRead);
        return Tuple2.of(readBuffers, shouldContinueRead);
    }

    @Override
    public long getPriority(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId,
            int bufferIndex) {
        lazyInitializeFileChannel();
        Tuple2<TieredStorageSubpartitionId, Integer> cacheKey =
                Tuple2.of(subpartitionId, bufferIndex);
        return tryGetCache(cacheKey, false)
                .map(BufferOffsetCache::getFileOffset)
                .orElse(Long.MAX_VALUE);
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
     * Try to get the cache according to the key.
     *
     * <p>If the relevant buffer offset cache exists, it will be returned and subsequently removed.
     * However, if the buffer offset cache does not exist, a new cache will be created using the
     * data index and returned.
     *
     * @param cacheKey the key of cache.
     * @param removeKey boolean decides whether to remove key.
     * @return returns the relevant buffer offset cache if it exists, otherwise return {@link
     *     Optional#empty()}.
     */
    private Optional<BufferOffsetCache> tryGetCache(
            Tuple2<TieredStorageSubpartitionId, Integer> cacheKey, boolean removeKey) {
        BufferOffsetCache bufferOffsetCache = bufferOffsetCaches.remove(cacheKey);
        if (bufferOffsetCache == null) {
            Optional<ProducerMergedPartitionFileIndex.FixedSizeRegion> regionOpt =
                    dataIndex.getRegion(cacheKey.f0, cacheKey.f1);
            return regionOpt.map(region -> new BufferOffsetCache(cacheKey.f1, region));
        } else {
            if (removeKey) {
                numCaches--;
            } else {
                bufferOffsetCaches.put(cacheKey, bufferOffsetCache);
            }
            return Optional.of(bufferOffsetCache);
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
     * @param partialBuffer the partial buffer, if the partial buffer is not null, it contains the
     *     partial data buffer from the previous read
     * @param readBuffers the read buffers list is to accept the sliced buffers
     * @return the first field is the partial data buffer, the second field is the number of sliced
     *     bytes.
     */
    private Tuple2<PartialBuffer, Integer> sliceBuffer(
            ByteBuffer byteBuffer,
            NetworkBuffer buffer,
            @Nullable PartialBuffer partialBuffer,
            List<Buffer> readBuffers) {
        checkState(reusedHeaderBuffer.position() == 0);

        int numSlicedBytes = 0;
        if (partialBuffer != null) {
            // If there is a previous small partial buffer, the current read operation should
            // read additional data and combine it with the existing partial to construct a new
            // complete buffer
            checkState(partialBuffer.missingLength() > 0);
            buffer.retainBuffer();
            int position = byteBuffer.position() + partialBuffer.missingLength();
            int numPartialBytes = partialBuffer.missingLength();
            partialBuffer.addPartialBuffer(
                    buffer.readOnlySlice(byteBuffer.position(), numPartialBytes));
            numSlicedBytes += numPartialBytes;
            byteBuffer.position(position);
            readBuffers.add(partialBuffer);
        }

        partialBuffer = null;
        while (byteBuffer.hasRemaining()) {
            // Parse the small buffer's header
            BufferHeader header = parseBufferHeader(byteBuffer, reusedHeaderBuffer);
            if (header == null) {
                // If the remaining length is not enough to parse a header, drop the remaining
                // byteBuffer directly
                break;
            } else {
                numSlicedBytes += HEADER_LENGTH;
            }

            if (header.getLength() <= byteBuffer.remaining()) {
                // The remaining data length in the buffer is enough to generate a new small
                // sliced network buffer. The small sliced buffer is not a partial buffer, we
                // should read the slice of the buffer directly
                buffer.retainBuffer();
                CompositeBuffer slicedBuffer = new CompositeBuffer(header);
                slicedBuffer.addPartialBuffer(
                        buffer.readOnlySlice(byteBuffer.position(), header.getLength()));
                byteBuffer.position(byteBuffer.position() + header.getLength());
                numSlicedBytes += header.getLength();
                readBuffers.add(slicedBuffer);
            } else {
                // The remaining data length in the buffer is smaller than the actual length of
                // the buffer, so we should generate a new partial buffer, allowing for
                // generating a new complete buffer during the next read operation
                if (byteBuffer.hasRemaining()) {
                    buffer.retainBuffer();
                    partialBuffer = new PartialBuffer(header);
                    int numPartialBytes = byteBuffer.remaining();
                    partialBuffer.addPartialBuffer(
                            buffer.readOnlySlice(byteBuffer.position(), numPartialBytes));
                    numSlicedBytes += numPartialBytes;
                    readBuffers.add(partialBuffer);
                }
                break;
            }
        }
        return Tuple2.of(partialBuffer, numSlicedBytes);
    }

    private void updateBufferOffsetCache(
            TieredStorageSubpartitionId subpartitionId,
            int bufferIndex,
            BufferOffsetCache cache,
            List<Buffer> readBuffers,
            @Nullable PartialBuffer partialBuffer,
            long regionFileStartOffset,
            long regionFileEndOffset,
            int numBytesRead,
            boolean shouldContinueRead) {
        int numFullBuffers = partialBuffer == null ? readBuffers.size() : readBuffers.size() - 1;
        int numBytesPartial =
                partialBuffer == null ? 0 : partialBuffer.readableBytes() + HEADER_LENGTH;
        // Note that the cache always stores the start offset of the full buffer, because the
        // partial buffer may be dropped anytime.
        long fullBufferFileOffset = regionFileStartOffset + numBytesRead - numBytesPartial;
        boolean hasNextBuffer = cache.advance(numFullBuffers, fullBufferFileOffset);

        checkState(hasNextBuffer == shouldContinueRead);
        checkState(
                !hasNextBuffer && fullBufferFileOffset == regionFileEndOffset
                        || fullBufferFileOffset < regionFileEndOffset);

        int nextBufferIndex = bufferIndex + numFullBuffers;
        if (hasNextBuffer && numCaches < maxCacheNumber) {
            // TODO: introduce the LRU cache strategy in the future to restrict the total cache
            // number. Testing to prevent cache leaks has been implemented.
            bufferOffsetCaches.put(Tuple2.of(subpartitionId, nextBufferIndex), cache);
            numCaches++;
        }
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
        checkArgument(reusedHeaderBuffer.position() == 0);

        BufferHeader header = null;
        try {
            if (buffer.remaining() >= HEADER_LENGTH) {
                // The remaining data length in the buffer is enough to construct a new complete
                // buffer, parse and create a new buffer header
                header = BufferReaderWriterUtil.parseBufferHeader(buffer);
            }
            // If the remaining data length in the buffer is smaller than the header. Drop it
            // directly
        } catch (Throwable throwable) {
            reusedHeaderBuffer.clear();
            LOG.error("Failed to parse buffer header.", throwable);
            throw throwable;
        }
        reusedHeaderBuffer.clear();
        return header;
    }

    /**
     * The {@link BufferOffsetCache} represents the file offset cache for a buffer index. Each cache
     * includes file offset of the buffer index, the region containing the buffer index and next
     * buffer index to consume.
     */
    private class BufferOffsetCache {

        private final ProducerMergedPartitionFileIndex.FixedSizeRegion region;

        private long fileOffset;

        private int nextBufferIndex;

        private BufferOffsetCache(
                int bufferIndex, ProducerMergedPartitionFileIndex.FixedSizeRegion region) {
            this.nextBufferIndex = bufferIndex;
            this.region = region;
            moveFileOffsetToBuffer(bufferIndex);
        }

        /**
         * Get the file offset.
         *
         * @return the file offset.
         */
        private long getFileOffset() {
            return fileOffset;
        }

        /**
         * Updates the {@link BufferOffsetCache} upon the retrieval of a buffer from the file using
         * the file offset in the {@link BufferOffsetCache}.
         *
         * @param numBuffers denotes the number of the advanced buffers.
         * @param newFileOffset indicate the advanced new file offset.
         * @return return true if there are remaining buffers in the region, otherwise return false.
         */
        private boolean advance(int numBuffers, long newFileOffset) {
            nextBufferIndex += numBuffers;
            fileOffset = newFileOffset;
            return nextBufferIndex < (region.getFirstBufferIndex() + region.getNumBuffers());
        }

        /**
         * Relocates the file channel offset to the position of the specified buffer index.
         *
         * @param bufferIndex denotes the index of the buffer.
         */
        private void moveFileOffsetToBuffer(int bufferIndex) {
            try {
                checkNotNull(fileChannel).position(region.getRegionFileOffset());
                for (int i = 0; i < (bufferIndex - region.getFirstBufferIndex()); ++i) {
                    positionToNextBuffer(fileChannel, reusedHeaderBuffer);
                }
                fileOffset = fileChannel.position();
                reusedHeaderBuffer.clear();
            } catch (IOException e) {
                ExceptionUtils.rethrow(e, "Failed to move file offset");
            }
        }
    }
}
