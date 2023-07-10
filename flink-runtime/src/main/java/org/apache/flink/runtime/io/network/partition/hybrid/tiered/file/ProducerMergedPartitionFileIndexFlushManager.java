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
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;
import java.util.function.BiConsumer;

import static org.apache.flink.runtime.io.network.partition.hybrid.InternalRegionWriteReadUtils.allocateAndConfigureBuffer;

/**
 * The {@link ProducerMergedPartitionFileIndexFlushManager} is to flush the regions to disk and load
 * the regions from disk.
 */
public class ProducerMergedPartitionFileIndexFlushManager {

    /** Reusable buffer used to read and write the immutable part of region. */
    private final ByteBuffer regionHeaderBuffer =
            allocateAndConfigureBuffer(ProducerMergedPartitionFileIndex.Region.REGION_SIZE);

    /**
     * List of subpartition's segment meta. Each element is a treeMap contains all {@link
     * ProducerMergedPartitionFileIndexFlushManager.SegmentMeta}'s of specific subpartition
     * corresponding to the subscript. The value of this treeMap is a {@link
     * ProducerMergedPartitionFileIndexFlushManager.SegmentMeta}, and the key is minBufferIndex of
     * this segment. Only finished(i.e. no longer appended) segment will be put to here.
     */
    private final List<TreeMap<Integer, ProducerMergedPartitionFileIndexFlushManager.SegmentMeta>>
            subpartitionFinishedSegmentMetas;

    private FileChannel channel;

    /** The Offset of next segment, new segment will start from this offset. */
    private long nextSegmentOffset = 0L;

    private final long[] subpartitionCurrentOffset;

    /** Free space of every subpartition's current segment. */
    private final int[] subpartitionFreeSpaceInBytes;

    /** Metadata of every subpartition's current segment. */
    private final ProducerMergedPartitionFileIndexFlushManager.SegmentMeta[] currentSegmentMeta;

    /**
     * Default size of segment. If the size of a region is larger than this value, it will be
     * allocated and occupy a single segment.
     */
    private final int segmentSizeInBytes;

    /**
     * This consumer is used to load region to cache. The first parameter is subpartition id, and
     * second parameter is the region to load.
     */
    private final BiConsumer<Integer, ProducerMergedPartitionFileIndex.Region> cacheRegionConsumer;

    /**
     * When region in segment needs to be loaded to cache, whether to load all regions of the entire
     * segment.
     */
    private final boolean loadEntireSegmentToCache;

    public ProducerMergedPartitionFileIndexFlushManager(
            int numSubpartitions,
            Path indexFilePath,
            int segmentSizeInBytes,
            long maxCacheCapacity,
            BiConsumer<Integer, ProducerMergedPartitionFileIndex.Region> cacheRegionConsumer) {
        try {
            this.channel =
                    FileChannel.open(
                            indexFilePath,
                            StandardOpenOption.CREATE_NEW,
                            StandardOpenOption.READ,
                            StandardOpenOption.WRITE);
        } catch (IOException e) {
            ExceptionUtils.rethrow(e);
        }
        this.loadEntireSegmentToCache =
                shouldLoadEntireSegmentToCache(
                        numSubpartitions, segmentSizeInBytes, maxCacheCapacity);
        this.subpartitionFinishedSegmentMetas = new ArrayList<>(numSubpartitions);
        this.subpartitionCurrentOffset = new long[numSubpartitions];
        this.subpartitionFreeSpaceInBytes = new int[numSubpartitions];
        this.currentSegmentMeta =
                new ProducerMergedPartitionFileIndexFlushManager.SegmentMeta[numSubpartitions];
        for (int i = 0; i < numSubpartitions; i++) {
            subpartitionFinishedSegmentMetas.add(new TreeMap<>());
        }
        this.cacheRegionConsumer = cacheRegionConsumer;
        this.segmentSizeInBytes = segmentSizeInBytes;
    }

    public long findRegion(int subpartition, int bufferIndex, boolean loadToCache) {
        // first of all, find the region from current writing segment.
        ProducerMergedPartitionFileIndexFlushManager.SegmentMeta segmentMeta =
                currentSegmentMeta[subpartition];
        if (segmentMeta != null) {
            long regionOffset =
                    findRegionInSegment(subpartition, bufferIndex, segmentMeta, loadToCache);
            if (regionOffset != -1) {
                return regionOffset;
            }
        }

        // next, find the region from finished segments.
        TreeMap<Integer, ProducerMergedPartitionFileIndexFlushManager.SegmentMeta>
                subpartitionSegmentMetaTreeMap = subpartitionFinishedSegmentMetas.get(subpartition);
        // all segments with a minBufferIndex less than or equal to this target buffer index may
        // contain the target region.
        for (ProducerMergedPartitionFileIndexFlushManager.SegmentMeta meta :
                subpartitionSegmentMetaTreeMap.headMap(bufferIndex, true).values()) {
            long regionOffset = findRegionInSegment(subpartition, bufferIndex, meta, loadToCache);
            if (regionOffset != -1) {
                return regionOffset;
            }
        }
        return -1;
    }

    private long findRegionInSegment(
            int subpartition,
            int bufferIndex,
            ProducerMergedPartitionFileIndexFlushManager.SegmentMeta meta,
            boolean loadToCache) {
        if (bufferIndex <= meta.getMaxBufferIndex()) {
            try {
                return readSegmentAndLoadToCacheIfNeeded(
                        subpartition, bufferIndex, meta, loadToCache);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        // -1 indicates that target region is not founded from this segment
        return -1;
    }

    private long readSegmentAndLoadToCacheIfNeeded(
            int subpartition,
            int bufferIndex,
            ProducerMergedPartitionFileIndexFlushManager.SegmentMeta meta,
            boolean loadToCache)
            throws IOException {
        // read all regions belong to this segment.
        List<Tuple2<ProducerMergedPartitionFileIndex.Region, Long>> regionAndOffsets =
                readSegment(meta.getOffset(), meta.getNumRegions());
        // -1 indicates that target region is not founded from this segment.
        long targetRegionOffset = -1;
        ProducerMergedPartitionFileIndex.Region targetRegion = null;
        // traverse all regions to find target.
        Iterator<Tuple2<ProducerMergedPartitionFileIndex.Region, Long>> it =
                regionAndOffsets.iterator();
        while (it.hasNext()) {
            Tuple2<ProducerMergedPartitionFileIndex.Region, Long> regionAndOffset = it.next();
            ProducerMergedPartitionFileIndex.Region region = regionAndOffset.f0;
            // whether the region contains this buffer.
            if (region.containBuffer(bufferIndex)) {
                // target region is founded.
                targetRegion = region;
                targetRegionOffset = regionAndOffset.f1;
                it.remove();
            }
        }

        // target region is founded and need to load to cache.
        if (targetRegion != null && loadToCache) {
            if (loadEntireSegmentToCache) {
                // first of all, load all regions except target to cache.
                regionAndOffsets.forEach(
                        (regionAndOffsetTuple) ->
                                cacheRegionConsumer.accept(subpartition, regionAndOffsetTuple.f0));
                // load target region to cache in the end, this is to prevent the target
                // from being eliminated.
                cacheRegionConsumer.accept(subpartition, targetRegion);
            } else {
                // only load target region to cache.
                cacheRegionConsumer.accept(subpartition, targetRegion);
            }
        }
        // return the offset of target region.
        return targetRegionOffset;
    }

    public void appendOrOverwriteRegion(
            int subpartition, ProducerMergedPartitionFileIndex.Region newRegion)
            throws IOException {
        // This method will only be called when we want to eliminate a region. We can't let the
        // region be reloaded into the cache, otherwise it will lead to an infinite loop.
        long oldRegionOffset = findRegion(subpartition, newRegion.getFirstBufferIndex(), false);
        if (oldRegionOffset != -1) {
            // if region is already exists in file, overwrite it.
            writeRegionToOffset(oldRegionOffset, newRegion);
        } else {
            // otherwise, append region to segment.
            appendRegion(subpartition, newRegion);
        }
    }

    public void close() throws IOException {
        if (channel != null) {
            channel.close();
        }
    }

    private static boolean shouldLoadEntireSegmentToCache(
            int numSubpartitions, int segmentSizeInBytes, long maxCacheCapacity) {
        // If the cache can put at least two segments (one for reading and one for writing) for each
        // subpartition, it is reasonable to load the entire segment into memory, which can improve
        // the cache hit rate. On the contrary, if the cache capacity is small, loading a large
        // number of regions will lead to performance degradation,only the target region should be
        // loaded.
        return ((long) 2 * numSubpartitions * segmentSizeInBytes)
                        / ProducerMergedPartitionFileIndex.Region.REGION_SIZE
                <= maxCacheCapacity;
    }

    private void appendRegion(int subpartition, ProducerMergedPartitionFileIndex.Region region)
            throws IOException {
        int regionSize = ProducerMergedPartitionFileIndex.Region.REGION_SIZE;
        // check whether we have enough space to append this region.
        if (subpartitionFreeSpaceInBytes[subpartition] < regionSize) {
            // No enough free space, start a new segment. Note that if region is larger than
            // segment's size, this will start a new segment only contains the big region.
            startNewSegment(subpartition, Math.max(regionSize, segmentSizeInBytes));
        }
        // spill this region to current offset of file index.
        writeRegionToOffset(subpartitionCurrentOffset[subpartition], region);
        // a new region was appended to segment, update it.
        updateSegment(subpartition, region);
    }

    private void writeRegionToOffset(long offset, ProducerMergedPartitionFileIndex.Region region)
            throws IOException {
        channel.position(offset);
        ProducerMergedPartitionFileIndex.writeRegionToFile(channel, regionHeaderBuffer, region);
    }

    private void startNewSegment(int subpartition, int newSegmentSize) {
        ProducerMergedPartitionFileIndexFlushManager.SegmentMeta oldSegmentMeta =
                currentSegmentMeta[subpartition];
        currentSegmentMeta[subpartition] =
                new ProducerMergedPartitionFileIndexFlushManager.SegmentMeta(nextSegmentOffset);
        subpartitionCurrentOffset[subpartition] = nextSegmentOffset;
        nextSegmentOffset += newSegmentSize;
        subpartitionFreeSpaceInBytes[subpartition] = newSegmentSize;
        if (oldSegmentMeta != null) {
            // put the finished segment to subpartitionFinishedSegmentMetas.
            subpartitionFinishedSegmentMetas
                    .get(subpartition)
                    .put(oldSegmentMeta.minBufferIndex, oldSegmentMeta);
        }
    }

    private void updateSegment(int subpartition, ProducerMergedPartitionFileIndex.Region region) {
        int regionSize = ProducerMergedPartitionFileIndex.Region.REGION_SIZE;
        subpartitionFreeSpaceInBytes[subpartition] -= regionSize;
        subpartitionCurrentOffset[subpartition] += regionSize;
        ProducerMergedPartitionFileIndexFlushManager.SegmentMeta segmentMeta =
                currentSegmentMeta[subpartition];
        segmentMeta.addRegion(
                region.getFirstBufferIndex(),
                region.getFirstBufferIndex() + region.getNumBuffers() - 1);
    }

    /**
     * Read segment from index file.
     *
     * @param offset offset of this segment.
     * @param numRegions number of regions of this segment.
     * @return List of all regions and its offset belong to this segment.
     */
    private List<Tuple2<ProducerMergedPartitionFileIndex.Region, Long>> readSegment(
            long offset, int numRegions) throws IOException {
        List<Tuple2<ProducerMergedPartitionFileIndex.Region, Long>> regionAndOffsets =
                new ArrayList<>();
        for (int i = 0; i < numRegions; i++) {
            ProducerMergedPartitionFileIndex.Region region =
                    ProducerMergedPartitionFileIndex.readRegionFromFile(
                            channel, regionHeaderBuffer, offset);
            regionAndOffsets.add(Tuple2.of(region, offset));
            offset += ProducerMergedPartitionFileIndex.Region.REGION_SIZE;
        }
        return regionAndOffsets;
    }

    /**
     * Metadata of spilled regions segment. When a segment is finished(i.e. no longer appended), its
     * corresponding {@link ProducerMergedPartitionFileIndexFlushManager.SegmentMeta} becomes
     * immutable.
     */
    private static class SegmentMeta {
        /**
         * Minimum buffer index of this segment. It is the smallest bufferIndex(inclusive) in all
         * regions belong to this segment.
         */
        private int minBufferIndex;

        /**
         * Maximum buffer index of this segment. It is the largest bufferIndex(inclusive) in all
         * regions belong to this segment.
         */
        private int maxBufferIndex;

        /** Number of regions belong to this segment. */
        private int numRegions;

        /** The index file offset of this segment. */
        private final long offset;

        public SegmentMeta(long offset) {
            this.offset = offset;
            this.minBufferIndex = Integer.MAX_VALUE;
            this.maxBufferIndex = 0;
            this.numRegions = 0;
        }

        public int getMaxBufferIndex() {
            return maxBufferIndex;
        }

        public long getOffset() {
            return offset;
        }

        public int getNumRegions() {
            return numRegions;
        }

        public void addRegion(int firstBufferIndexOfRegion, int maxBufferIndexOfRegion) {
            if (firstBufferIndexOfRegion < minBufferIndex) {
                this.minBufferIndex = firstBufferIndexOfRegion;
            }
            if (maxBufferIndexOfRegion > maxBufferIndex) {
                this.maxBufferIndex = maxBufferIndexOfRegion;
            }
            this.numRegions++;
        }
    }

    /** Factory of {@link ProducerMergedPartitionFileIndexFlushManager}. */
    public static class Factory {
        private final int segmentSizeInBytes;

        private final long maxCacheCapacity;

        public Factory(int segmentSizeInBytes, long maxCacheCapacity) {
            this.segmentSizeInBytes = segmentSizeInBytes;
            this.maxCacheCapacity = maxCacheCapacity;
        }

        public ProducerMergedPartitionFileIndexFlushManager create(
                int numSubpartitions,
                Path indexFilePath,
                BiConsumer<Integer, ProducerMergedPartitionFileIndex.Region> cacheRegionConsumer) {
            return new ProducerMergedPartitionFileIndexFlushManager(
                    numSubpartitions,
                    indexFilePath,
                    segmentSizeInBytes,
                    maxCacheCapacity,
                    cacheRegionConsumer);
        }
    }
}
