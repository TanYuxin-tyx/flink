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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.function.Consumer;

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

    private final ByteBuffer reusedHeaderBuffer = BufferReaderWriterUtil.allocatedHeaderBuffer();

    private final Path dataFilePath;

    private final ProducerMergedPartitionFileIndex dataIndex;

    private final int maxCacheNumber;

    private volatile FileChannel fileChannel;

    private BufferAndHeader partialBuffer;

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
    public boolean readBuffer(
            boolean shouldPrintLog,
            String taskName,
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId,
            int bufferIndex,
            Queue<MemorySegment> buffers,
            BufferRecycler recycler,
            Consumer<Buffer> bufferConsumer)
            throws IOException {

        MemorySegment memorySegment = buffers.poll();
        lazyInitializeFileChannel();
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
        Optional<BufferOffsetCache> cache = tryGetCache(cacheKey, true);
        if (!cache.isPresent()) {
            buffers.add(memorySegment);
            return false;
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
        fileChannel.position(cache.get().getFileOffset());
        if (shouldPrintLog) {
            LOG.error("### " + taskName + " position to buffer");
        }

        long currentReadOffset = cache.get().fileOffset;
        long regionFileEndOffset = cache.get().region.getRegionFileEndOffset();

        int numBytes =
                (int) Math.min(memorySegment.size(), regionFileEndOffset - currentReadOffset);
        ByteBuffer byteBuffer = memorySegment.wrap(0, numBytes);

        try {
            BufferReaderWriterUtil.readByteBufferFully(fileChannel, byteBuffer);
            byteBuffer.flip();
        } catch (Throwable throwable) {
            recycler.recycle(memorySegment);
            throw throwable;
        }

        NetworkBuffer networkBuffer = new NetworkBuffer(memorySegment, recycler);
        networkBuffer.setSize(byteBuffer.remaining());

        partialBuffer = new BufferAndHeader(null, null);
        try {
            partialBuffer =
                    processBuffer(
                            byteBuffer,
                            networkBuffer,
                            wrappedBufferConsumer(
                                    bufferConsumer, cache, subpartitionId, bufferIndex));
        } catch (Throwable throwable) {
            partialBuffer = new BufferAndHeader(null, null);
            throw throwable;
        } finally {
            networkBuffer.recycleBuffer();
            if (reusedHeaderBuffer.position() > 0) {
                reusedHeaderBuffer.clear();
            }
            if (partialBuffer.buffer != null) {
                partialBuffer.buffer.recycleBuffer();
            }
        }
        return true;
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

    Consumer<Buffer> wrappedBufferConsumer(
            Consumer<Buffer> originalBufferConsumer,
            Optional<BufferOffsetCache> cache,
            TieredStorageSubpartitionId subpartitionId,
            int bufferIndex) {
        checkState(cache.isPresent());
        return buffer -> {
            originalBufferConsumer.accept(buffer);
            boolean hasNextBuffer =
                    cache.get()
                            .advance(
                                    checkNotNull(buffer).readableBytes()
                                            + BufferReaderWriterUtil.HEADER_LENGTH);
            if (hasNextBuffer) {
                int nextBufferIndex = bufferIndex + 1;
                // TODO: introduce the LRU cache strategy in the future to restrict the total
                // cache number. Testing to prevent cache leaks has been implemented.
                if (numCaches < maxCacheNumber) {
                    bufferOffsetCaches.put(Tuple2.of(subpartitionId, nextBufferIndex), cache.get());
                    numCaches++;
                }
            }
        };
    }

    private BufferAndHeader processBuffer(
            ByteBuffer byteBuffer, Buffer buffer, Consumer<Buffer> consumer) {
        BufferHeader header = partialBuffer.header;
        CompositeBuffer targetBuffer = partialBuffer.buffer;
        while (byteBuffer.hasRemaining()) {
            if (header == null && (header = parseBufferHeader(byteBuffer)) == null) {
                break;
            }

            if (targetBuffer != null) {
                buffer.retainBuffer();
                int position = byteBuffer.position() + targetBuffer.missingLength();
                targetBuffer.addPartialBuffer(
                        buffer.readOnlySlice(byteBuffer.position(), targetBuffer.missingLength()));
                byteBuffer.position(position);
            } else if (byteBuffer.remaining() < header.getLength()) {
                if (byteBuffer.hasRemaining()) {
                    buffer.retainBuffer();
                    targetBuffer = new CompositeBuffer(header);
                    targetBuffer.addPartialBuffer(
                            buffer.readOnlySlice(byteBuffer.position(), byteBuffer.remaining()));
                }
                break;
            } else {
                buffer.retainBuffer();
                targetBuffer = new CompositeBuffer(header);
                targetBuffer.addPartialBuffer(
                        buffer.readOnlySlice(byteBuffer.position(), header.getLength()));
                byteBuffer.position(byteBuffer.position() + header.getLength());
            }

            header = null;
            consumer.accept(targetBuffer);
            targetBuffer = null;
        }
        return new BufferAndHeader(targetBuffer, header);
    }

    private BufferHeader parseBufferHeader(ByteBuffer buffer) {
        BufferHeader header = null;
        if (reusedHeaderBuffer.position() > 0) {
            while (reusedHeaderBuffer.hasRemaining()) {
                reusedHeaderBuffer.put(buffer.get());
            }
            reusedHeaderBuffer.flip();
            header = BufferReaderWriterUtil.parseBufferHeader(reusedHeaderBuffer);
            reusedHeaderBuffer.clear();
        }

        if (header == null && buffer.remaining() < HEADER_LENGTH) {
            reusedHeaderBuffer.put(buffer);
        } else if (header == null) {
            header = BufferReaderWriterUtil.parseBufferHeader(buffer);
        }
        return header;
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

    private static class BufferAndHeader {

        private final CompositeBuffer buffer;
        private final BufferHeader header;

        BufferAndHeader(CompositeBuffer buffer, BufferHeader header) {
            this.buffer = buffer;
            this.header = header;
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
         * @param bufferSize denotes the size of the buffer.
         * @return return true if there are remaining buffers in the region, otherwise return false.
         */
        private boolean advance(long bufferSize) {
            nextBufferIndex++;
            fileOffset += bufferSize;
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
            } catch (IOException e) {
                ExceptionUtils.rethrow(e, "Failed to move file offset");
            }
        }
    }
}
