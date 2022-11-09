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

package org.apache.flink.runtime.io.network.partition.store.local.file;

import org.apache.flink.runtime.io.network.partition.store.tier.local.file.RegionBufferIndexTracker;
import org.apache.flink.runtime.io.network.partition.store.tier.local.file.RegionBufferIndexTracker.ReadableRegion;
import org.apache.flink.runtime.io.network.partition.store.tier.local.file.RegionBufferIndexTracker.SpilledBuffer;
import org.apache.flink.runtime.io.network.partition.store.tier.local.file.RegionBufferIndexTrackerImpl;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RegionBufferIndexTrackerImpl}. */
@ExtendWith(TestLoggerExtension.class)
class RegionBufferIndexTrackerImplTest {
    private static final int NUM_SUBPARTITIONS = 2;

    private RegionBufferIndexTracker regionBufferIndexTracker;

    @BeforeEach
    void before() {
        regionBufferIndexTracker = new RegionBufferIndexTrackerImpl(NUM_SUBPARTITIONS);
    }

    /**
     * If the buffer index with the corresponding subpartition does not exist in the data index, or
     * no buffer has ever been added to the subpartition. The return value should be {@link
     * Optional#empty()}.
     */
    @Test
    void testGetReadableRegionBufferNotExist() {
        regionBufferIndexTracker.addBuffers(createSpilledBuffers(0, Arrays.asList(0, 2)));
        regionBufferIndexTracker.markBufferReleased(0, 0);
        regionBufferIndexTracker.markBufferReleased(0, 2);

        // subpartition 0 does not have buffer with index 1
        assertThat(regionBufferIndexTracker.getReadableRegion(0, 1, -1)).isNotPresent();

        // subpartition 1 has no buffer
        assertThat(regionBufferIndexTracker.getReadableRegion(1, 0, -1)).isNotPresent();
    }

    /** If target buffer is not readable, {@link Optional#empty()} should be eventually returned. */
    @Test
    void testGetReadableRegionNotReadable() {
        regionBufferIndexTracker.addBuffers(createSpilledBuffers(0, Collections.singletonList(0)));
        regionBufferIndexTracker.markBufferReleased(0, 0);

        // 0-0 is not readable as consuming offset is bigger than 0.
        assertThat(regionBufferIndexTracker.getReadableRegion(0, 0, 1)).isNotPresent();
    }

    /** If target buffer is not released, {@link Optional#empty()} should be eventually returned. */
    @Test
    void testGetReadableRegionNotReleased() {
        regionBufferIndexTracker.addBuffers(createSpilledBuffers(0, Collections.singletonList(0)));

        // 0-0 is not released
        assertThat(regionBufferIndexTracker.getReadableRegion(0, 0, -1)).isNotPresent();
    }

    /**
     * If target buffer is already readable, a not null {@link ReadableRegion} starts with the given
     * buffer index should be returned.
     */
    @Test
    void testGetReadableRegion() {
        final int subpartitionId = 0;

        regionBufferIndexTracker.addBuffers(
                createSpilledBuffers(subpartitionId, Arrays.asList(0, 1, 3, 4, 5)));
        regionBufferIndexTracker.markBufferReleased(subpartitionId, 1);
        regionBufferIndexTracker.markBufferReleased(subpartitionId, 3);
        regionBufferIndexTracker.markBufferReleased(subpartitionId, 4);

        assertThat(regionBufferIndexTracker.getReadableRegion(subpartitionId, 1, 0))
                .hasValueSatisfying(
                        readableRegion -> {
                            assertRegionStartWithTargetBufferIndex(readableRegion, 1);
                            // Readable region will not include discontinuous buffer.
                            assertThat(readableRegion.numReadable).isEqualTo(1);
                        });
        assertThat(regionBufferIndexTracker.getReadableRegion(subpartitionId, 3, 0))
                .hasValueSatisfying(
                        readableRegion -> {
                            assertRegionStartWithTargetBufferIndex(readableRegion, 3);
                            assertThat(readableRegion.numReadable)
                                    .isGreaterThanOrEqualTo(1)
                                    .isLessThanOrEqualTo(2);
                        });
        assertThat(regionBufferIndexTracker.getReadableRegion(subpartitionId, 4, 0))
                .hasValueSatisfying(
                        readableRegion -> {
                            assertRegionStartWithTargetBufferIndex(readableRegion, 4);
                            assertThat(readableRegion.numReadable).isEqualTo(1);
                        });
    }

    /**
     * Verify that the offset of the first buffer of the region is the offset of the target buffer.
     */
    private static void assertRegionStartWithTargetBufferIndex(
            ReadableRegion readableRegion, int targetBufferIndex) {
        assertThat(targetBufferIndex).isEqualTo(readableRegion.offset + readableRegion.numSkip);
    }

    /** Note that: To facilitate testing, offset are set to be equal to buffer index. */
    private static List<SpilledBuffer> createSpilledBuffers(
            int subpartitionId, List<Integer> bufferIndexes) {
        List<SpilledBuffer> spilledBuffers = new ArrayList<>();
        for (int bufferIndex : bufferIndexes) {
            spilledBuffers.add(new SpilledBuffer(subpartitionId, bufferIndex, bufferIndex));
        }
        return spilledBuffers;
    }
}
