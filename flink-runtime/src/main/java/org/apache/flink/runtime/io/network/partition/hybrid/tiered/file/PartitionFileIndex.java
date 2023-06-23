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
 * The {@link PartitionFileIndex} is responsible for storing the indexes of data files generated
 * during the partition file write process and utilized during partition file reads. In order to
 * simplify the representation of consecutive buffers that belong to a single subpartition within a
 * file, these indexes are encapsulated into a {@link Region}. During the partition file write
 * process, the {@link Region}s are generated based on the buffers. During partition file reads, the
 * {@link Region} is used to retrieve consecutive buffers that belong to a single subpartition.
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
     * Based on the input {@link BufferToFlush}s, generate the {@link Region}s accordingly. When the
     * buffer's subpartition id changes or the buffer index changes, a new region is created. For
     * example, the buffers are as follows(each buffer is represented by
     * subpartitionId-bufferIndex). 1-1, 1-2, 1-3, 2-1, 2-2, 2-5, 1-4, 1-5, 2-6, then 5 regions are
     * generated. |1-1, 1-2, 1-3|2-1, 2-2|2-5|1-4, 1-5|2-6|.
     *
     * <p>Note that these buffers are logically partitioned by the region indexes logically, but
     * they remain physically contiguous when flushing to disk.
     *
     * @param bufferToFlushes the buffers to be flushed
     */
    void generateRegionsBasedOnBuffers(List<BufferToFlush> bufferToFlushes) {
        if (bufferToFlushes.isEmpty()) {
            return;
        }

        Map<Integer, List<Region>> convertedRegions = convertToRegions(bufferToFlushes);
        synchronized (lock) {
            convertedRegions.forEach(
                    (subpartition, regions) ->
                            subpartitionRegions.get(subpartition).addAll(regions));
        }
    }

    /**
     * Get the {@link Region} of the specific subpartition.
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

    private static Map<Integer, List<Region>> convertToRegions(
            List<BufferToFlush> bufferToFlushes) {
        Map<Integer, List<Region>> subpartitionRegionMap = new HashMap<>();
        Iterator<BufferToFlush> iterator = bufferToFlushes.iterator();
        BufferToFlush firstBufferInRegion = iterator.next();
        BufferToFlush lastBufferInRegion = firstBufferInRegion;

        while (iterator.hasNext()) {
            BufferToFlush currentBuffer = iterator.next();
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
            BufferToFlush firstBufferInRegion,
            BufferToFlush lastBufferInRegion,
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

    // ------------------------------------------------------------------------
    //  Internal Classes
    // ------------------------------------------------------------------------

    /** Represents a buffer to be flushed. */
    public static class BufferToFlush {
        /** The subpartition id that the buffer belongs to. */
        private final int subpartitionId;

        /** The buffer index within the subpartition. */
        private final int bufferIndex;

        /** The file offset that the buffer begin with. */
        private final long fileOffset;

        BufferToFlush(int subpartitionId, int bufferIndex, long fileOffset) {
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
