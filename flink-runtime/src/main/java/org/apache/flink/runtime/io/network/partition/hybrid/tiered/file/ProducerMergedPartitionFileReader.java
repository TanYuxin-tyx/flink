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
 * shuffle data is written in the producer side with a single file.
 */
public class ProducerMergedPartitionFileReader implements PartitionFileReader {

    private static final int CACHE_MAX_NUM = 10000;

    private final Map<Tuple2<TieredStorageSubpartitionId, Integer>, RegionCache> regionCaches;

    private final ByteBuffer reusedHeaderBuffer = BufferReaderWriterUtil.allocatedHeaderBuffer();

    private final Path dataFilePath;

    private FileChannel fileChannel;

    private final ProducerMergedPartitionFileIndex dataIndex;

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
        RegionCache regionCache = regionCaches.get(cacheKey);
        if (regionCache == null) {
            Optional<Region> region =
                    dataIndex.getRegion(subpartitionId.getSubpartitionId(), bufferIndex);
            if (region.isPresent()) {
                regionCache = new RegionCache(bufferIndex, region.get());
                regionCaches.put(cacheKey, regionCache);
            } else {
                return null;
            }
        }
        long fileOffSet = regionCache.getCurrentFileOffset();
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

        if (!hasBuffer) {
            int nextBufferIndex = bufferIndex + 1;
            Optional<Region> region =
                    dataIndex.getRegion(subpartitionId.getSubpartitionId(), nextBufferIndex);
            region.ifPresent(value -> regionCaches.put(
                    Tuple2.of(subpartitionId, nextBufferIndex),
                    new RegionCache(nextBufferIndex, value)));
        }
        regionCaches.remove(cacheKey);
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
        return regionCache == null ? Long.MAX_VALUE : regionCache.getCurrentFileOffset();
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
        fileChannel = null;
        IOUtils.deleteFileQuietly(dataFilePath);
    }

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
     * {@link RegionCache} is the cache to record the reading progress for a consumer of a
     * subpartition.
     */
    private class RegionCache {

        private final Region region;

        private long currentFileOffset;

        private int numBuffersReadable;

        public RegionCache(
                int bufferIndex, Region region) {
            this.region = region;
            moveFileOffsetToBuffer(bufferIndex);
        }

        /**
         * Get the current file offset.
         *
         * @return the file offset.
         */
        private long getCurrentFileOffset() {
            return currentFileOffset;
        }

        private boolean advance(long bufferSize) {
            numBuffersReadable--;
            currentFileOffset += bufferSize;
            return numBuffersReadable != 0;
        }

        private void moveFileOffsetToBuffer(int bufferIndex) {
            try {
                checkNotNull(fileChannel).position(region.getRegionFileOffset());
                for (int i = 0; i < (bufferIndex - region.getFirstBufferIndex()); ++i) {
                    positionToNextBuffer(fileChannel, reusedHeaderBuffer);
                }
                currentFileOffset = fileChannel.position();
            } catch (IOException e) {
                ExceptionUtils.rethrow(e, "Failed to move file offset");
            }
            int skipBuffers = bufferIndex - region.getFirstBufferIndex();
            numBuffersReadable = region.getNumBuffers() - skipBuffers;
        }
    }
}
