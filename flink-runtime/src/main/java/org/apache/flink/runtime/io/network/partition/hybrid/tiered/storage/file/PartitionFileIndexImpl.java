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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file;

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionId;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Default implementation of {@link PartitionFileIndex}. */
public class PartitionFileIndexImpl implements PartitionFileIndex {

    /**
     * The regions belonging to each subpartitions.
     *
     * <p>Note that the field can be accessed by the writing and reading IO thread, so the lock is
     * to ensure the thread safety.
     */
    @GuardedBy("lock")
    private final List<List<Region>> subpartitionRegions;

    /**
     * The region index of a reader is reading for each subpartition. The list index is
     * corresponding to the subpartition id. The key in the map represents the reader, the value in
     * the map represents the reading region index.
     */
    @GuardedBy("lock")
    private final List<Map<NettyConnectionId, Integer>> subpartitionReaderRegionIndexes;

    @GuardedBy("lock")
    private boolean isReleased;

    private final Object lock = new Object();

    public PartitionFileIndexImpl(int numSubpartitions) {
        this.subpartitionRegions = new ArrayList<>();
        this.subpartitionReaderRegionIndexes = new ArrayList<>();
        for (int subpartitionId = 0; subpartitionId < numSubpartitions; ++subpartitionId) {
            subpartitionRegions.add(new ArrayList<>());
            subpartitionReaderRegionIndexes.add(new HashMap<>());
        }
    }

    @Override
    public Optional<Region> getNextRegion(
            int subpartitionId, NettyConnectionId nettyServiceWriterId) {
        synchronized (lock) {
            if (isReleased) {
                return Optional.empty();
            }
            // return the latest region
            int currentRegionIndex =
                    subpartitionReaderRegionIndexes
                            .get(subpartitionId)
                            .getOrDefault(nettyServiceWriterId, 0);
            List<Region> currentRegions = subpartitionRegions.get(subpartitionId);
            if (currentRegionIndex < currentRegions.size()) {
                Region region = currentRegions.get(currentRegionIndex);
                ++currentRegionIndex;
                subpartitionReaderRegionIndexes
                        .get(subpartitionId)
                        .put(nettyServiceWriterId, currentRegionIndex);
                return Optional.of(region);
            }
            return Optional.empty();
        }
    }

    @Override
    public void addRegionIndex(List<SpilledBuffer> spilledBuffers) {
        final Map<Integer, List<Region>> subpartitionInternalRegions =
                convertToRegions(spilledBuffers);
        synchronized (lock) {
            subpartitionInternalRegions.forEach(
                    (subpartition, internalRegions) -> {
                        List<Region> regionList = subpartitionRegions.get(subpartition);
                        regionList.addAll(internalRegions);
                    });
        }
    }

    @Override
    public void release() {
        synchronized (lock) {
            subpartitionRegions.clear();
            subpartitionReaderRegionIndexes.clear();
            isReleased = true;
        }
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    private static Map<Integer, List<Region>> convertToRegions(List<SpilledBuffer> spilledBuffers) {
        if (spilledBuffers.isEmpty()) {
            return Collections.emptyMap();
        }

        final Map<Integer, List<Region>> subpartitionRegions = new HashMap<>();
        final Iterator<SpilledBuffer> iterator = spilledBuffers.iterator();
        // There's at least one buffer
        SpilledBuffer firstBufferInRegion = iterator.next();
        SpilledBuffer lastBufferInRegion = firstBufferInRegion;

        while (iterator.hasNext()) {
            SpilledBuffer currentBuffer = iterator.next();
            if (currentBuffer.subpartitionId != firstBufferInRegion.subpartitionId
                    || currentBuffer.bufferIndex != lastBufferInRegion.bufferIndex + 1) {
                // the current buffer belongs to a new region, close the previous region
                addInternalRegionToMap(
                        firstBufferInRegion, lastBufferInRegion, subpartitionRegions);
                firstBufferInRegion = currentBuffer;
            }

            lastBufferInRegion = currentBuffer;
        }

        addInternalRegionToMap(firstBufferInRegion, lastBufferInRegion, subpartitionRegions);
        return subpartitionRegions;
    }

    private static void addInternalRegionToMap(
            SpilledBuffer firstBufferInRegion,
            SpilledBuffer lastBufferInRegion,
            Map<Integer, List<Region>> internalRegionsBySubpartition) {
        checkArgument(firstBufferInRegion.subpartitionId == lastBufferInRegion.subpartitionId);
        checkArgument(firstBufferInRegion.bufferIndex <= lastBufferInRegion.bufferIndex);
        internalRegionsBySubpartition
                .computeIfAbsent(firstBufferInRegion.subpartitionId, ArrayList::new)
                .add(
                        new Region(
                                firstBufferInRegion.bufferIndex,
                                firstBufferInRegion.fileOffset,
                                lastBufferInRegion.bufferIndex
                                        - firstBufferInRegion.bufferIndex
                                        + 1));
    }
}
