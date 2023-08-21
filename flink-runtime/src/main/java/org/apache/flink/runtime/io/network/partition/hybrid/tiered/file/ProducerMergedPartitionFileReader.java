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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
    public List<Buffer> readBuffer(
            boolean shouldPrintLog,
            String taskName,
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

        List<Buffer> readBuffers = new LinkedList<>();
        Tuple2<TieredStorageSubpartitionId, Integer> cacheKey =
                Tuple2.of(subpartitionId, bufferIndex);
        if (shouldPrintLog) {
            LOG.error(
                    "### "
                            + taskName
                            + " try get cache, datafile: "
                            + dataFilePath
                            + " size:"
                            + fileChannel.size());
        }
        Optional<BufferOffsetCache> cache =
                tryGetCache(cacheKey, reusedHeaderBuffer, partialBuffer, true);
        if (!cache.isPresent()) {
            return null;
        }
        if (shouldPrintLog) {
            LOG.error(
                    "### "
                            + taskName
                            + " get cache "
                            + cache.get().fileOffset
                            + " nextBufferIndex:"
                            + cache.get().nextBufferIndex
                            + " region num buffers: "
                            + cache.get().region.getNumBuffers()
                            + " region first buffer index: "
                            + cache.get().region.getFirstBufferIndex()
                            + " region file offset: "
                            + cache.get().region.getRegionFileOffset()
                            + " region file end offset: "
                            + cache.get().region.getRegionFileEndOffset()
                            + " region size: "
                            + cache.get().region.getSize());
        }

        long regionFileStartOffset =
                partialBuffer == null ? cache.get().getFileOffset() : partialBuffer.getFileOffset();
        long regionFileEndOffset = cache.get().region.getRegionFileEndOffset();
        checkState(regionFileStartOffset <= regionFileEndOffset);
        int numBytesToRead =
                Math.min(memorySegment.size(), (int) (regionFileEndOffset - regionFileStartOffset));
        if (numBytesToRead == 0) {
            return null;
        }
        LOG.info(
                "###"
                        + taskName
                        + " reading from file offset: "
                        + " partial buffer: "
                        + partialBuffer
                        + " cache offset: "
                        + cache.get().getFileOffset()
                        + " partial buffer offset: "
                        + (partialBuffer == null ? "null" : partialBuffer.getFileOffset())
                        + regionFileStartOffset
                        + " num bytes to read:"
                        + numBytesToRead
                        + " region end offset: "
                        + regionFileEndOffset);
        ByteBuffer byteBuffer = memorySegment.wrap(0, numBytesToRead);
        fileChannel.position(regionFileStartOffset);

        try {
            BufferReaderWriterUtil.readByteBufferFully(fileChannel, byteBuffer);
            byteBuffer.flip();
        } catch (Throwable throwable) {
            recycler.recycle(memorySegment);
            throw throwable;
        }

        NetworkBuffer buffer = new NetworkBuffer(memorySegment, recycler);
        buffer.setSize(byteBuffer.remaining());
        int numFullBuffers;
        try {
            Tuple2<CompositeBuffer, BufferHeader> partial =
                    splitBuffer(
                            taskName,
                            byteBuffer,
                            buffer,
                            reusedHeaderBuffer,
                            partialBuffer,
                            readBuffers);
            numFullBuffers = readBuffers.size();
            if (regionFileStartOffset + numBytesToRead < regionFileEndOffset) {
                partialBuffer =
                        new PartialBuffer(
                                regionFileStartOffset + numBytesToRead, partial.f0, partial.f1);
                LOG.info(
                        "###"
                                + taskName
                                + " next file offset: "
                                + (regionFileStartOffset + numBytesToRead)
                                + " region end offset: "
                                + regionFileEndOffset);
                readBuffers.add(partialBuffer);
            } else {
                LOG.info(
                        "###"
                                + taskName
                                + " no more data in region, next file offset: "
                                + (regionFileStartOffset + numBytesToRead)
                                + " region end offset: "
                                + regionFileEndOffset);
                checkState(partial.f0 == null);
            }
        } catch (Throwable throwable) {
            throw throwable;
        } finally {
            buffer.recycleBuffer();
        }

        boolean hasNextBuffer = cache.get().advanceBytes(numBytesToRead, numFullBuffers);
        checkState(
                !hasNextBuffer && regionFileStartOffset + numBytesToRead == regionFileEndOffset
                        || regionFileStartOffset + numBytesToRead < regionFileEndOffset);
        int nextBufferIndex = bufferIndex + numFullBuffers;
        if (hasNextBuffer && numCaches < maxCacheNumber) {
            // TODO: introduce the LRU cache strategy in the future to restrict the total cache
            // number. Testing to prevent cache leaks has been implemented.
            bufferOffsetCaches.put(Tuple2.of(subpartitionId, nextBufferIndex), cache.get());
            numCaches++;
        }

        return readBuffers;
        //            if (hasNextBuffer) {
        //                int nextBufferIndex = bufferIndex + 1;
        //                // TODO: introduce the LRU cache strategy in the future to restrict the
        // total
        //                // cache number. Testing to prevent cache leaks has been implemented.
        //                if (numCaches < maxCacheNumber) {
        //                    bufferOffsetCaches.put(Tuple2.of(subpartitionId, nextBufferIndex),
        // cache.get());
        //                    numCaches++;
        //                }
        //            }
        //            return Collections.singletonList(buffer);
        //        } else {
        //            // TODO
        //            return null;
        //        }
    }

    private Tuple2<CompositeBuffer, BufferHeader> splitBuffer(
            String taskName,
            ByteBuffer byteBuffer,
            NetworkBuffer buffer,
            ByteBuffer reusedHeaderBuffer,
            @Nullable PartialBuffer partialBuffer,
            List<Buffer> readBuffers) {
        BufferHeader header = partialBuffer == null ? null : partialBuffer.getBufferHeader();
        CompositeBuffer slicedBuffer =
                partialBuffer == null ? null : partialBuffer.getCompositeBuffer();
        LOG.error(
                "###"
                        + taskName
                        + "start split buffer "
                        + byteBuffer.remaining()
                        + " header: "
                        + header
                        + " reused header buffer position:"
                        + reusedHeaderBuffer.position()
                        + " partial buffer: "
                        + partialBuffer
                        + " slicedBuffer: "
                        + slicedBuffer
                        + " sliced missing len:"
                        + (slicedBuffer == null ? 0 : slicedBuffer.missingLength())
                        + " sliced len:"
                        + (slicedBuffer == null ? 0 : slicedBuffer.readableBytes())
                        + " header: "
                        + header
                        + (header == null
                                ? ""
                                : " "
                                        + header.getLength()
                                        + " "
                                        + header.getDataType()
                                        + " "
                                        + header.isCompressed()));
        if (header == null) {
            checkState(slicedBuffer == null || reusedHeaderBuffer.position() > 0);
        }
        checkState(slicedBuffer == null || slicedBuffer.missingLength() > 0);

        while (byteBuffer.hasRemaining()) {
            // Parse the small buffer's header
            if (header == null
                    && (header = parseBufferHeader(taskName, byteBuffer, reusedHeaderBuffer))
                            == null) {
                LOG.error(
                        "###"
                                + taskName
                                + " break the loop, header is null,: "
                                + reusedHeaderBuffer.position()
                                + " byteBuffer remain:"
                                + byteBuffer.remaining());
                break;
            }

            // If the previous partial buffer is not exist
            if (slicedBuffer == null) {
                if (header.getLength() <= byteBuffer.remaining()) {
                    // The new small is complete, it is not a partial buffer, we should read the
                    // sliced buffer directly
                    buffer.retainBuffer();
                    slicedBuffer = new CompositeBuffer(header);
                    slicedBuffer.addPartialBuffer(
                            buffer.readOnlySlice(byteBuffer.position(), header.getLength()));
                    byteBuffer.position(byteBuffer.position() + header.getLength());
                } else {
                    // The new small buffer is not complete, it is a real partial buffer
                    if (byteBuffer.hasRemaining()) {
                        buffer.retainBuffer();
                        slicedBuffer = new CompositeBuffer(header);
                        slicedBuffer.addPartialBuffer(
                                buffer.readOnlySlice(
                                        byteBuffer.position(), byteBuffer.remaining()));
                    }
                    LOG.info(
                            "###"
                                    + taskName
                                    + " creating a partial buffer"
                                    + " sliced "
                                    + (slicedBuffer == null
                                            ? "null"
                                            : +slicedBuffer.readableBytes()
                                                    + " "
                                                    + slicedBuffer.isBuffer()
                                                    + " "
                                                    + slicedBuffer.missingLength())
                                    + " current size: "
                                    + readBuffers.size()
                                    + " remaining: "
                                    + byteBuffer.remaining());
                    break;
                }
            } else {
                // If there is a previous small partial buffer, we should complete the partial
                // buffer firstly
                buffer.retainBuffer();
                LOG.info(
                        "###"
                                + taskName
                                + " before position to new position: "
                                + byteBuffer.position()
                                + " sliced buffer len: "
                                + slicedBuffer.readableBytes()
                                + " "
                                + slicedBuffer.isBuffer()
                                + " "
                                + slicedBuffer.missingLength()
                                + " current size: "
                                + readBuffers.size()
                                + " remaining: "
                                + byteBuffer.remaining());
                int position = byteBuffer.position() + slicedBuffer.missingLength();
                slicedBuffer.addPartialBuffer(
                        buffer.readOnlySlice(byteBuffer.position(), slicedBuffer.missingLength()));
                byteBuffer.position(position);
                LOG.info(
                        "###"
                                + taskName
                                + " position to new position: "
                                + position
                                + " sliced buffer len: "
                                + slicedBuffer.readableBytes()
                                + " "
                                + slicedBuffer.isBuffer()
                                + " "
                                + slicedBuffer.missingLength()
                                + " current size: "
                                + readBuffers.size()
                                + " remaining: "
                                + byteBuffer.remaining());
            }

            header = null;
            readBuffers.add(slicedBuffer);
            LOG.info(
                    "###"
                            + taskName
                            + " add a new sliced buffer"
                            + slicedBuffer.readableBytes()
                            + " "
                            + slicedBuffer.isBuffer()
                            + " "
                            + slicedBuffer.missingLength()
                            + " current size: "
                            + readBuffers.size()
                            + " remaining: "
                            + byteBuffer.remaining());
            slicedBuffer = null;
        }
        LOG.error(
                "###"
                        + taskName
                        + " byte buffer: "
                        + byteBuffer
                        + " "
                        + slicedBuffer
                        + " "
                        + (slicedBuffer == null
                                ? ""
                                : slicedBuffer.readableBytes()
                                        + " "
                                        + slicedBuffer.missingLength()
                                        + " "
                                        + slicedBuffer)
                        + " header "
                        + (header == null
                                ? ""
                                : header.getLength()
                                        + " "
                                        + header.getDataType()
                                        + " isCompressed: "
                                        + header.isCompressed()
                                        + " buffer: "
                                        + reusedHeaderBuffer
                                        + " reuse header position: "
                                        + reusedHeaderBuffer.position()
                                        + " remaining: "
                                        + reusedHeaderBuffer.remaining()));
        checkState(slicedBuffer == null || slicedBuffer.missingLength() > 0);
        return Tuple2.of(slicedBuffer, header);
    }

    private BufferHeader parseBufferHeader(
            String taskName, ByteBuffer buffer, ByteBuffer reusedHeaderBuffer) {
        BufferHeader header = null;
        try {
            if (reusedHeaderBuffer.position() >= HEADER_LENGTH) {
                reusedHeaderBuffer.clear();
            }
            if (reusedHeaderBuffer.position() > 0) {
                LOG.error(
                        "###"
                                + taskName
                                + " parseBufferHeader, position: "
                                + reusedHeaderBuffer.position()
                                + " buffer remain:"
                                + buffer.remaining());
                while (reusedHeaderBuffer.hasRemaining()) {
                    reusedHeaderBuffer.put(buffer.get());
                }
                LOG.error(
                        "###"
                                + taskName
                                + " parseBufferHeader, position: "
                                + reusedHeaderBuffer.position()
                                + " buffer remain:"
                                + buffer.remaining());
                reusedHeaderBuffer.flip();
                header = BufferReaderWriterUtil.parseBufferHeader(reusedHeaderBuffer);
                reusedHeaderBuffer.clear();
            }

            if (header == null && buffer.remaining() < HEADER_LENGTH) {
                LOG.error(
                        "###"
                                + taskName
                                + " parseBufferHeader, left some data, but not enough for header, remaining: "
                                + buffer.remaining()
                                + " header len:"
                                + HEADER_LENGTH);
                reusedHeaderBuffer.put(buffer);
            } else if (header == null) {
                header = BufferReaderWriterUtil.parseBufferHeader(buffer);
                LOG.error(
                        "###"
                                + taskName
                                + " parseBufferHeader, remaining: "
                                + buffer.remaining()
                                + " header len:"
                                + header.getLength()
                                + " datatype:"
                                + header.getDataType()
                                + " isCompressed: "
                                + header.isCompressed());
                reusedHeaderBuffer.clear();
            }
        } catch (Throwable throwable) {
            reusedHeaderBuffer.clear();
            LOG.error(
                    "###"
                            + taskName
                            + " parse header error, "
                            + header
                            + " remaining: "
                            + buffer.remaining()
                            + " position: "
                            + buffer.position()
                            + " "
                            + buffer,
                    throwable);
            throw throwable;
        }

        return header;
    }

    @Override
    public long getPriority(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId,
            int bufferIndex,
            ByteBuffer reusedHeaderBuffer) {
        lazyInitializeFileChannel();
        Tuple2<TieredStorageSubpartitionId, Integer> cacheKey =
                Tuple2.of(subpartitionId, bufferIndex);
        return tryGetCache(cacheKey, reusedHeaderBuffer, null, false)
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
            Tuple2<TieredStorageSubpartitionId, Integer> cacheKey,
            @Nullable ByteBuffer reusedHeaderBuffer,
            @Nullable PartialBuffer partialBuffer,
            boolean removeKey) {
        BufferOffsetCache bufferOffsetCache = bufferOffsetCaches.remove(cacheKey);
        if (bufferOffsetCache == null) {
            Optional<ProducerMergedPartitionFileIndex.FixedSizeRegion> regionOpt =
                    dataIndex.getRegion(cacheKey.f0, cacheKey.f1);
            return regionOpt.map(
                    region ->
                            new BufferOffsetCache(
                                    cacheKey.f1, region, reusedHeaderBuffer, partialBuffer));
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
     * The {@link BufferOffsetCache} represents the file offset cache for a buffer index. Each cache
     * includes file offset of the buffer index, the region containing the buffer index and next
     * buffer index to consume.
     */
    private class BufferOffsetCache {

        private final ProducerMergedPartitionFileIndex.FixedSizeRegion region;

        private long fileOffset;

        private int nextBufferIndex;

        private BufferOffsetCache(
                int bufferIndex,
                ProducerMergedPartitionFileIndex.FixedSizeRegion region,
                ByteBuffer reusedHeaderBuffer,
                @Nullable PartialBuffer partialBuffer) {
            this.nextBufferIndex = bufferIndex;
            this.region = region;
            if (partialBuffer != null) {
                fileOffset = partialBuffer.getFileOffset();
            } else {
                moveFileOffsetToBuffer(bufferIndex, reusedHeaderBuffer);
            }
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
         * @param bufferSize denotes the size of the buffer.
         * @return return true if there are remaining buffers in the region, otherwise return false.
         */
        private boolean advance(long bufferSize) {
            nextBufferIndex++;
            fileOffset += bufferSize;
            return nextBufferIndex < (region.getFirstBufferIndex() + region.getNumBuffers());
        }

        private boolean advance(long bufferSize, int numBuffers) {
            nextBufferIndex += numBuffers;
            fileOffset += bufferSize * numBuffers;
            return nextBufferIndex < (region.getFirstBufferIndex() + region.getNumBuffers());
        }

        private boolean advanceBytes(long readSizeBytes, int numBuffers) {
            fileOffset += readSizeBytes;
            nextBufferIndex += numBuffers;
            return nextBufferIndex < (region.getFirstBufferIndex() + region.getNumBuffers());
        }

        /**
         * Relocates the file channel offset to the position of the specified buffer index.
         *
         * @param bufferIndex denotes the index of the buffer.
         */
        private void moveFileOffsetToBuffer(int bufferIndex, ByteBuffer reusedHeaderBuffer) {
            try {
                checkNotNull(fileChannel).position(region.getRegionFileOffset());
                for (int i = 0; i < (bufferIndex - region.getFirstBufferIndex()); ++i) {
                    positionToNextBuffer(fileChannel, reusedHeaderBuffer);
                }
                fileOffset = fileChannel.position();
            } catch (IOException e) {
                ExceptionUtils.rethrow(e, "Failed to move file offset");
            }
        }
    }
}
