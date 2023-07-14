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

package org.apache.flink.runtime.io.network.partition.hybrid.region;

import org.apache.flink.runtime.io.network.partition.hybrid.HsFileDataIndexImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.TestingFileDataIndexSpilledRegionManager;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.apache.flink.runtime.io.network.partition.hybrid.HybridShuffleTestUtils.assertRegionEquals;
import static org.apache.flink.runtime.io.network.partition.hybrid.HybridShuffleTestUtils.createAllUnreleasedRegions;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FileDataIndexCache}. */
class FileDataIndexCacheTest {
    private FileDataIndexCache<HsFileDataIndexImpl.InternalRegion> indexCache;

    private TestingFileDataIndexSpilledRegionManager<HsFileDataIndexImpl.InternalRegion>
            spilledRegionManager;

    private final int numSubpartitions = 1;

    private int numRetainedIndexEntry = 10;

    @BeforeEach
    void before(@TempDir Path tmpPath) throws Exception {
        Path indexFilePath = Files.createFile(tmpPath.resolve(UUID.randomUUID().toString()));
        TestingFileDataIndexSpilledRegionManager.Factory<HsFileDataIndexImpl.InternalRegion>
                testingSpilledRegionManagerFactory =
                        new TestingFileDataIndexSpilledRegionManager.Factory<>();
        indexCache =
                new FileDataIndexCache<>(
                        numSubpartitions,
                        indexFilePath,
                        numRetainedIndexEntry,
                        testingSpilledRegionManagerFactory);
        spilledRegionManager = testingSpilledRegionManagerFactory.getLastSpilledRegionManager();
    }

    @Test
    void testPutAndGet() {
        indexCache.put(0, createAllUnreleasedRegions(0, 0L, 3, 1));
        Optional<HsFileDataIndexImpl.InternalRegion> regionOpt = indexCache.get(0, 0);
        assertThat(regionOpt)
                .hasValueSatisfying(
                        (region) -> {
                            assertThat(region.getFirstBufferIndex()).isEqualTo(0);
                            assertThat(region.getRegionFileOffset()).isEqualTo(0);
                            assertThat(region.getNumBuffers()).isEqualTo(3);
                        });
    }

    @Test
    void testCachedRegionRemovedWhenExceedsRetainedEntry(@TempDir Path tmpPath) throws Exception {
        numRetainedIndexEntry = 3;
        Path indexFilePath = Files.createFile(tmpPath.resolve(UUID.randomUUID().toString()));
        TestingFileDataIndexSpilledRegionManager.Factory<HsFileDataIndexImpl.InternalRegion>
                testingSpilledRegionManagerFactory =
                        new TestingFileDataIndexSpilledRegionManager.Factory<>();
        indexCache =
                new FileDataIndexCache<>(
                        numSubpartitions,
                        indexFilePath,
                        numRetainedIndexEntry,
                        testingSpilledRegionManagerFactory);
        spilledRegionManager = testingSpilledRegionManagerFactory.getLastSpilledRegionManager();

        // region 0, 1, 2
        List<HsFileDataIndexImpl.InternalRegion> regionList =
                createAllUnreleasedRegions(0, 0L, 3, 3);
        indexCache.put(0, regionList);
        assertThat(spilledRegionManager.getSpilledRegionSize(0)).isZero();

        // number of regions exceeds numRetainedIndexEntry, trigger cache purge.
        indexCache.put(0, createAllUnreleasedRegions(9, 9L, 3, 1));
        assertThat(spilledRegionManager.getSpilledRegionSize(0)).isEqualTo(1);
        HsFileDataIndexImpl.InternalRegion region = spilledRegionManager.getRegion(0, 0);
        assertThat(region).isNotNull();
        assertRegionEquals(region, regionList.get(0));
        // number of regions exceeds numRetainedIndexEntry, trigger cache purge.
        indexCache.put(0, createAllUnreleasedRegions(12, 12L, 3, 1));
        assertThat(spilledRegionManager.getSpilledRegionSize(0)).isEqualTo(2);
        HsFileDataIndexImpl.InternalRegion region2 = spilledRegionManager.getRegion(0, 3);
        assertThat(region2).isNotNull();
        assertRegionEquals(region2, regionList.get(1));
    }

    @Test
    void testGetNonExistentRegion() {
        Optional<HsFileDataIndexImpl.InternalRegion> region = indexCache.get(0, 0);
        // get a non-existent region.
        assertThat(region).isNotPresent();
    }

    @Test
    void testCacheLoadSpilledRegion(@TempDir Path tmpPath) throws Exception {
        numRetainedIndexEntry = 1;
        Path indexFilePath = Files.createFile(tmpPath.resolve(UUID.randomUUID().toString()));
        TestingFileDataIndexSpilledRegionManager.Factory<HsFileDataIndexImpl.InternalRegion>
                testingSpilledRegionManagerFactory =
                        new TestingFileDataIndexSpilledRegionManager.Factory<>();
        indexCache =
                new FileDataIndexCache<>(
                        numSubpartitions,
                        indexFilePath,
                        numRetainedIndexEntry,
                        testingSpilledRegionManagerFactory);
        spilledRegionManager = testingSpilledRegionManagerFactory.getLastSpilledRegionManager();

        indexCache.put(0, createAllUnreleasedRegions(0, 0L, 1, 2));
        assertThat(spilledRegionManager.getSpilledRegionSize(0)).isEqualTo(1);

        assertThat(spilledRegionManager.getFindRegionInvoked()).isZero();
        Optional<HsFileDataIndexImpl.InternalRegion> regionOpt = indexCache.get(0, 0);
        assertThat(spilledRegionManager.getSpilledRegionSize(0)).isEqualTo(2);
        assertThat(regionOpt).isPresent();
        assertThat(spilledRegionManager.getFindRegionInvoked()).isEqualTo(1);
        // previously get should already load this region to cache.
        assertThat(indexCache.get(0, 0)).isPresent();
        assertThat(spilledRegionManager.getFindRegionInvoked()).isEqualTo(1);
    }
}
