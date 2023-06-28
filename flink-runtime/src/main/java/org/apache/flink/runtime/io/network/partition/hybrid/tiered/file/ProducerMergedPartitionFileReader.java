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
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageIdMappingUtils;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IOUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.positionToNextBuffer;
import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.readFromByteChannel;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.ProducerMergedPartitionFileIndex.Region;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The implementation of {@link PartitionFileReader} with producer-merge mode. In this mode, the
 * shuffle data is written in the producer side, the consumer side need to read multiple producers
 * to get its partition data.
 *
 * <p>Note that one partition file may contain the data of multiple subpartitions.
 */
public class ProducerMergedPartitionFileReader implements PartitionFileReader {

    /**
     * Max number of region caches.
     *
     * <p>This number is utilized to constrain the total number of cached regions in the event of a
     * region leak within the implementation. Corresponding tests have been added to guarantee
     * correctness. As the future implementation approaches, the cache storage strategy will be
     * established using the LRU algorithm with a justifiable cache size.
     */
    private static final int MAX_REGION_CACHE = 10000;

    /**
     * Region caches stored in map.
     *
     * <p>The key of the cache is formed by combining the {@link TieredStorageSubpartitionId} and
     * buffer index. The value denotes the region cache for the corresponding subpartition and
     * buffer index. Each region cache comprises the last consumed {@link Region}, the next buffer
     * index to consume within the region, and the file offset of the next buffer index.
     */
    private final Map<Tuple2<TieredStorageSubpartitionId, Integer>, RegionCache> regionCaches;

    private final ByteBuffer reusedHeaderBuffer = BufferReaderWriterUtil.allocatedHeaderBuffer();

    private final Path dataFilePath;

    private final ProducerMergedPartitionFileIndex dataIndex;

    private FileChannel fileChannel;

    /** The current number of region caches. */
    private int numRegionCache;

    ProducerMergedPartitionFileReader(
            Path dataFilePath, ProducerMergedPartitionFileIndex dataIndex) {
        this.dataFilePath = dataFilePath;
        this.dataIndex = dataIndex;
        this.regionCaches = new HashMap<>();
    }

    @Override
    public Buffer readBuffer(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId,
            int bufferIndex,
            MemorySegment memorySegment,
            BufferRecycler recycler)
            throws IOException {

        lazyInitializeFileChannel(partitionId);
        Tuple2<TieredStorageSubpartitionId, Integer> cacheKey =
                Tuple2.of(subpartitionId, bufferIndex);
        RegionCache regionCache = tryGetRegionCache(cacheKey);
        if (regionCache == null) {
            return null;
        }
        long fileOffSet = regionCache.getFileOffset();
        try {
            fileChannel.position(fileOffSet);
        } catch (IOException e) {
            ExceptionUtils.rethrow(e, "Failed to position file offset to buffer.");
        }
        Buffer buffer = null;
        try {
            buffer = readFromByteChannel(fileChannel, reusedHeaderBuffer, memorySegment, recycler);
        } catch (IOException e) {
            ExceptionUtils.rethrow(e, "Failed to read buffer.");
        }
        boolean hasBuffer =
                regionCache.advance(
                        checkNotNull(buffer).readableBytes()
                                + BufferReaderWriterUtil.HEADER_LENGTH);
        if (hasBuffer) {
            int nextBufferIndex = bufferIndex + 1;
            if (numRegionCache < MAX_REGION_CACHE) {
                regionCaches.put(Tuple2.of(subpartitionId, nextBufferIndex), regionCache);
                numRegionCache++;
            }
        }
        return buffer;
    }

    @Override
    public long getPriority(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId,
            int bufferIndex) {
        Tuple2<TieredStorageSubpartitionId, Integer> cacheKey =
                Tuple2.of(subpartitionId, bufferIndex);
        RegionCache regionCache = regionCaches.get(cacheKey);
        return regionCache == null ? Long.MAX_VALUE : regionCache.getFileOffset();
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
     *
     * @param partitionId denotes the partition id.
     * @throws IOException is thrown in the event of an error.
     */
    private void lazyInitializeFileChannel(TieredStoragePartitionId partitionId)
            throws IOException {
        if (fileChannel == null) {
            try {
                fileChannel = FileChannel.open(dataFilePath, StandardOpenOption.READ);
            } catch (FileNotFoundException e) {
                throw new PartitionNotFoundException(
                        TieredStorageIdMappingUtils.convertId(partitionId));
            }
        }
    }

    /**
     * Try to get the region cache associated with the given cache key.
     *
     * <p>If the relevant region cache exists within the region caches, it will be returned and
     * subsequently removed. However, if the region cache does not exist, a new region cache will be
     * created using the data index and returned.
     *
     * @param cacheKey denotes the key of the desired region cache.
     * @return returns the relevant region cache if it exists, otherwise a null value.
     * @throws IOException is thrown in the event of an error.
     */
    private RegionCache tryGetRegionCache(Tuple2<TieredStorageSubpartitionId, Integer> cacheKey)
            throws IOException {
        RegionCache regionCache = regionCaches.remove(cacheKey);
        if (regionCache == null) {
            Optional<Region> region = dataIndex.getRegion(cacheKey.f0, cacheKey.f1);
            if (region.isPresent()) {
                regionCache = new RegionCache(cacheKey.f1, region.get());
            }
        } else {
            numRegionCache--;
        }
        return regionCache;
    }

    /**
     * The {@link RegionCache} represents the cache of a {@link Region}. Each region cache contains
     * the last consumed {@link Region}, the next buffer index to consume within the region, and the
     * file offset of the buffer index.
     */
    private class RegionCache {

        private final Region region;

        private int nextBufferIndex;

        private long fileOffset;

        private RegionCache(int bufferIndex, Region region) throws IOException {
            this.nextBufferIndex = bufferIndex;
            this.region = region;
            moveFileOffsetToBuffer(bufferIndex);
        }

        /**
         * Get the file offset of next buffer.
         *
         * @return the file offset.
         */
        private long getFileOffset() {
            return fileOffset;
        }

        /**
         * Updates the {@link RegionCache} upon the retrieval of a buffer from the file using the
         * file offset in the {@link RegionCache}.
         *
         * @param bufferSize denotes the size of the buffer.
         * @return returns a boolean value indicating the presence or absence of residual buffers in
         *     the region.
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
         * @throws IOException is thrown in the event of an error.
         */
        private void moveFileOffsetToBuffer(int bufferIndex) throws IOException {
            checkNotNull(fileChannel).position(region.getRegionFileOffset());
            for (int i = 0; i < (bufferIndex - region.getFirstBufferIndex()); ++i) {
                positionToNextBuffer(fileChannel, reusedHeaderBuffer);
            }
            fileOffset = fileChannel.position();
        }
    }
}
