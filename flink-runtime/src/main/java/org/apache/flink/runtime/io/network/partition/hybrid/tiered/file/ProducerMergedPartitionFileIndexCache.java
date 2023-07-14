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

/**
 * A cache layer of producer merged partition file index. This class encapsulates the logic of the
 * index's put and get, and automatically caches some indexes in memory. When there are too many
 * cached indexes, it will decide and flush some indexes to disk.
 */
public class ProducerMergedPartitionFileIndexCache {
    //    /**
    //     * This struct stores all in memory {@link ProducerMergedPartitionFileIndex.Region}s. Each
    //     * element is a treeMap contains all in memory {@link
    // ProducerMergedPartitionFileIndex.Region}'s
    //     * of specific subpartition corresponding to the subscript. The value of this treeMap is a
    //     * {@link ProducerMergedPartitionFileIndex.Region}, and the key is firstBufferIndex of
    // this
    //     * region. Only cached in memory region will be put to here.
    //     */
    //    private final List<TreeMap<Integer, ProducerMergedPartitionFileIndex.Region>>
    //            subpartitionFirstBufferIndexInternalRegions;
    //
    //    /**
    //     * This cache is used to help eliminate regions from memory. It is only maintains the key
    // of
    //     * each in memory region, the value is just a placeholder. Note that this internal cache
    // must be
    //     * consistent with subpartitionFirstBufferIndexInternalRegions, that means both of them
    // must add
    //     * or delete elements at the same time.
    //     */
    //    private final Cache<ProducerMergedPartitionFileIndexCache.CachedRegionKey, Object>
    //            internalCache;
    //
    //    private final ProducerMergedPartitionFileIndexFlushManager spilledRegionManager;
    //
    //    private final Path indexFilePath;
    //
    //    /**
    //     * Placeholder of cache entry's value. Because the cache is only used for managing
    // region's
    //     * elimination, does not need the real region as value.
    //     */
    //    public static final Object PLACEHOLDER = new Object();
    //
    //    public ProducerMergedPartitionFileIndexCache(
    //            int numSubpartitions,
    //            Path indexFilePath,
    //            long numRetainedInMemoryRegionsMax,
    //            ProducerMergedPartitionFileIndexFlushManager.Factory spilledRegionManagerFactory)
    // {
    //        this.subpartitionFirstBufferIndexInternalRegions = new ArrayList<>(numSubpartitions);
    //        for (int subpartitionId = 0; subpartitionId < numSubpartitions; ++subpartitionId) {
    //            subpartitionFirstBufferIndexInternalRegions.add(new TreeMap<>());
    //        }
    //        this.internalCache =
    //                CacheBuilder.newBuilder()
    //                        .maximumSize(numRetainedInMemoryRegionsMax)
    //                        .removalListener(this::handleRemove)
    //                        .build();
    //        this.indexFilePath = checkNotNull(indexFilePath);
    //        this.spilledRegionManager =
    //                spilledRegionManagerFactory.create(
    //                        numSubpartitions,
    //                        indexFilePath,
    //                        (subpartition, region) -> {
    //                            if (!getCachedRegionContainsTargetBufferIndex(
    //                                            subpartition, region.getFirstBufferIndex())
    //                                    .isPresent()) {
    //                                subpartitionFirstBufferIndexInternalRegions
    //                                        .get(subpartition)
    //                                        .put(region.getFirstBufferIndex(), region);
    //                                internalCache.put(
    //                                        new
    // ProducerMergedPartitionFileIndexCache.CachedRegionKey(
    //                                                subpartition, region.getFirstBufferIndex()),
    //                                        PLACEHOLDER);
    //                            } else {
    //                                // this is needed for cache entry remove algorithm like LRU.
    //                                internalCache.getIfPresent(
    //                                        new
    // ProducerMergedPartitionFileIndexCache.CachedRegionKey(
    //                                                subpartition, region.getFirstBufferIndex()));
    //                            }
    //                        });
    //    }
    //
    //    /**
    //     * Get a region contains target bufferIndex and belong to target subpartition.
    //     *
    //     * @param subpartitionId the subpartition that target buffer belong to.
    //     * @param bufferIndex the index of target buffer.
    //     * @return If target region can be founded from memory or disk, return optional contains
    // target
    //     *     region. Otherwise, return {@code Optional#empty()};
    //     */
    //    public Optional<ProducerMergedPartitionFileIndex.Region> get(
    //            int subpartitionId, int bufferIndex) {
    //        // first of all, try to get region in memory.
    //        Optional<ProducerMergedPartitionFileIndex.Region> regionOpt =
    //                getCachedRegionContainsTargetBufferIndex(subpartitionId, bufferIndex);
    //        if (regionOpt.isPresent()) {
    //            ProducerMergedPartitionFileIndex.Region region = regionOpt.get();
    //            checkNotNull(
    //                    // this is needed for cache entry remove algorithm like LRU.
    //                    internalCache.getIfPresent(
    //                            new ProducerMergedPartitionFileIndexCache.CachedRegionKey(
    //                                    subpartitionId, region.getFirstBufferIndex())));
    //            return Optional.of(region);
    //        } else {
    //            // try to find target region and load it into cache if founded.
    //            spilledRegionManager.findRegion(subpartitionId, bufferIndex, true);
    //            return getCachedRegionContainsTargetBufferIndex(subpartitionId, bufferIndex);
    //        }
    //    }
    //
    //    /**
    //     * Put regions to cache.
    //     *
    //     * @param subpartition the subpartition's id of regions.
    //     * @param internalRegions regions to be cached.
    //     */
    //    public void put(
    //            int subpartition, List<ProducerMergedPartitionFileIndex.Region> internalRegions) {
    //        TreeMap<Integer, ProducerMergedPartitionFileIndex.Region> treeMap =
    //                subpartitionFirstBufferIndexInternalRegions.get(subpartition);
    //        for (ProducerMergedPartitionFileIndex.Region internalRegion : internalRegions) {
    //            internalCache.put(
    //                    new ProducerMergedPartitionFileIndexCache.CachedRegionKey(
    //                            subpartition, internalRegion.getFirstBufferIndex()),
    //                    PLACEHOLDER);
    //            treeMap.put(internalRegion.getFirstBufferIndex(), internalRegion);
    //        }
    //    }
    //
    //    /**
    //     * Close {@link ProducerMergedPartitionFileIndexCache}, this will delete the index file.
    // After
    //     * that, the index can no longer be read or written.
    //     */
    //    public void close() throws IOException {
    //        spilledRegionManager.close();
    //        IOUtils.deleteFileQuietly(indexFilePath);
    //    }
    //
    //    // This is a callback after internal cache removed an entry from itself.
    //    private void handleRemove(
    //            RemovalNotification<ProducerMergedPartitionFileIndexCache.CachedRegionKey, Object>
    //                    removedEntry) {
    //        ProducerMergedPartitionFileIndexCache.CachedRegionKey removedKey =
    // removedEntry.getKey();
    //        // remove the corresponding region from memory.
    //        ProducerMergedPartitionFileIndex.Region removedRegion =
    //                subpartitionFirstBufferIndexInternalRegions
    //                        .get(removedKey.getSubpartition())
    //                        .remove(removedKey.getFirstBufferIndex());
    //
    //        // write this region to file. After that, no strong reference point to this region, it
    // can
    //        // be safely released by gc.
    //        writeRegion(removedKey.getSubpartition(), removedRegion);
    //    }
    //
    //    private void writeRegion(int subpartition, ProducerMergedPartitionFileIndex.Region region)
    // {
    //        try {
    //            spilledRegionManager.appendOrOverwriteRegion(subpartition, region);
    //        } catch (IOException e) {
    //            ExceptionUtils.rethrow(e);
    //        }
    //    }
    //
    //    /**
    //     * Get the cached in memory region contains target buffer.
    //     *
    //     * @param subpartitionId the subpartition that target buffer belong to.
    //     * @param bufferIndex the index of target buffer.
    //     * @return If target region is cached in memory, return optional contains target region.
    //     *     Otherwise, return {@code Optional#empty()};
    //     */
    //    private Optional<ProducerMergedPartitionFileIndex.Region>
    //            getCachedRegionContainsTargetBufferIndex(int subpartitionId, int bufferIndex) {
    //        return Optional.ofNullable(
    //                        subpartitionFirstBufferIndexInternalRegions
    //                                .get(subpartitionId)
    //                                .floorEntry(bufferIndex))
    //                .map(Map.Entry::getValue)
    //                .filter(internalRegion -> internalRegion.containBuffer(bufferIndex));
    //    }
    //
    //    /**
    //     * This class represents the key of cached region, it is uniquely identified by the
    // region's
    //     * subpartition id and firstBufferIndex.
    //     */
    //    private static class CachedRegionKey {
    //        /** The subpartition id of cached region. */
    //        private final int subpartition;
    //
    //        /** The first buffer's index of cached region. */
    //        private final int firstBufferIndex;
    //
    //        public CachedRegionKey(int subpartition, int firstBufferIndex) {
    //            this.subpartition = subpartition;
    //            this.firstBufferIndex = firstBufferIndex;
    //        }
    //
    //        public int getSubpartition() {
    //            return subpartition;
    //        }
    //
    //        public int getFirstBufferIndex() {
    //            return firstBufferIndex;
    //        }
    //
    //        @Override
    //        public boolean equals(Object o) {
    //            if (this == o) {
    //                return true;
    //            }
    //            if (o == null || getClass() != o.getClass()) {
    //                return false;
    //            }
    //            ProducerMergedPartitionFileIndexCache.CachedRegionKey that =
    //                    (ProducerMergedPartitionFileIndexCache.CachedRegionKey) o;
    //            return subpartition == that.subpartition && firstBufferIndex ==
    // that.firstBufferIndex;
    //        }
    //
    //        @Override
    //        public int hashCode() {
    //            return Objects.hash(subpartition, firstBufferIndex);
    //        }
    //    }
}
