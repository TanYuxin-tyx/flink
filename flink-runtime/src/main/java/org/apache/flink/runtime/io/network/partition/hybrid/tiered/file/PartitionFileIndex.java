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

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * The {@link PartitionFileIndex} represents the indexes and the regions of the spilled buffers. The
 * region indexes are generated when writing the spilled buffers, and these region indexes are used
 * when reading data from disk.
 */
public class PartitionFileIndex {

    /**
     * The regions belonging to each subpartitions.
     *
     * <p>Note that the field can be accessed by the writing and reading IO thread, so the lock is
     * to ensure the thread safety.
     */
    @GuardedBy("lock")
    private final List<List<Region>> subpartitionRegions;

    @GuardedBy("lock")
    private boolean isReleased;

    private final Object lock = new Object();

    public PartitionFileIndex(int numSubpartitions) {
        this.subpartitionRegions = new ArrayList<>();
        for (int subpartitionId = 0; subpartitionId < numSubpartitions; ++subpartitionId) {
            subpartitionRegions.add(new ArrayList<>());
        }
    }

    /**
     * Based on the input {@link SpilledBuffer}s, generate the region accordingly. When the spilled
     * buffer's subpartition id changes, a new region is created. For example, the buffers to be
     * spilled are as follows(each buffer is represented by subpartitionId-bufferIndex). 1-1, 1-2,
     * 1-3, 2-1, 2-2, 2-5, 1-4, 1-5, 2-6, then 5 regions are generated. |1-1, 1-2, 1-3|2-1,
     * 2-2|2-5|1-4, 1-5|2-6|.
     *
     * <p>Note that these spilled buffers are logically partitioned by the region indexes logically,
     * but they remain physically contiguous when flushing to disk.
     *
     * @param spilledBuffers the buffers to be spilled
     */
    void generateRegionsBasedOnBuffers(List<SpilledBuffer> spilledBuffers) {
        if (spilledBuffers.isEmpty()) {
            return;
        }

        Map<Integer, List<Region>> convertedRegions = convertToRegions(spilledBuffers);
        synchronized (lock) {
            convertedRegions.forEach(
                    (subpartition, regions) ->
                            subpartitionRegions.get(subpartition).addAll(regions));
        }
    }

    /**
     * Get the {@link Region} of the specific supbpartition.
     *
     * @param subpartitionId the specific subpartition id
     * @param regionId the region id to get from the {@link PartitionFileIndex}
     */
    public Optional<Region> getRegion(int subpartitionId, int regionId) {
        synchronized (lock) {
            if (isReleased) {
                return Optional.empty();
            }
            List<Region> currentRegions = subpartitionRegions.get(subpartitionId);
            if (regionId < currentRegions.size()) {
                return Optional.of(currentRegions.get(regionId));
            }
            return Optional.empty();
        }
    }

    public void release() {
        synchronized (lock) {
            subpartitionRegions.clear();
            isReleased = true;
        }
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    private static Map<Integer, List<Region>> convertToRegions(List<SpilledBuffer> spilledBuffers) {
        Map<Integer, List<Region>> subpartitionRegionMap = new HashMap<>();
        Iterator<SpilledBuffer> iterator = spilledBuffers.iterator();
        SpilledBuffer firstBufferInRegion = iterator.next();
        SpilledBuffer lastBufferInRegion = firstBufferInRegion;

        while (iterator.hasNext()) {
            SpilledBuffer currentBuffer = iterator.next();
            if (currentBuffer.getSubpartitionId() != firstBufferInRegion.getSubpartitionId()
                    || currentBuffer.getBufferIndex() != lastBufferInRegion.getBufferIndex() + 1) {
                // the current buffer belongs to a new region, close the previous region
                addInternalRegionToMap(
                        firstBufferInRegion, lastBufferInRegion, subpartitionRegionMap);
                firstBufferInRegion = currentBuffer;
            }
            lastBufferInRegion = currentBuffer;
        }

        addInternalRegionToMap(firstBufferInRegion, lastBufferInRegion, subpartitionRegionMap);
        return subpartitionRegionMap;
    }

    private static void addInternalRegionToMap(
            SpilledBuffer firstBufferInRegion,
            SpilledBuffer lastBufferInRegion,
            Map<Integer, List<Region>> subpartitionRegionMap) {
        checkArgument(
                firstBufferInRegion.getSubpartitionId() == lastBufferInRegion.getSubpartitionId());
        checkArgument(firstBufferInRegion.getBufferIndex() <= lastBufferInRegion.getBufferIndex());

        subpartitionRegionMap
                .computeIfAbsent(firstBufferInRegion.getSubpartitionId(), ArrayList::new)
                .add(
                        new Region(
                                firstBufferInRegion.getFileOffset(),
                                lastBufferInRegion.getBufferIndex()
                                        - firstBufferInRegion.getBufferIndex()
                                        + 1));
    }

    /** Represents a buffer to be spilled. */
    public static class SpilledBuffer {
        /** The subpartition id that the buffer belongs to. */
        private final int subpartitionId;

        /** The buffer index within the subpartition. */
        private final int bufferIndex;

        /** The file offset that the buffer begin with. */
        private final long fileOffset;

        SpilledBuffer(int subpartitionId, int bufferIndex, long fileOffset) {
            this.subpartitionId = subpartitionId;
            this.bufferIndex = bufferIndex;
            this.fileOffset = fileOffset;
        }

        public int getSubpartitionId() {
            return subpartitionId;
        }

        public int getBufferIndex() {
            return bufferIndex;
        }

        public long getFileOffset() {
            return fileOffset;
        }
    }

    /**
     * A {@link Region} represents a series of physically continuous buffers in the file, which are
     * from the same subpartition.
     */
    public static class Region {

        /** The file offset of the region. */
        private final long regionFileOffset;

        /** The number of buffers that the region contains. */
        private final int numBuffers;

        Region(long regionFileOffset, int numBuffers) {
            this.regionFileOffset = regionFileOffset;
            this.numBuffers = numBuffers;
        }

        public long getRegionFileOffset() {
            return regionFileOffset;
        }

        public int getNumBuffers() {
            return numBuffers;
        }
    }
}
