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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.common;

import org.apache.flink.util.function.SupplierWithException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The implementation of {@link SubpartitionSegmentIndexTracker}. Each {@link TierStorage}'s data
 * manager has a separate {@link SubpartitionSegmentIndexTrackerImpl}.
 */
public class SubpartitionSegmentIndexTrackerImpl implements SubpartitionSegmentIndexTracker {

    // Each subpartition calculates the amount of data written to a tier separately. If the
    // amount of data exceeds the threshold, the segment is switched. Different subpartitions
    // may have duplicate segment indexes, so it is necessary to distinguish different
    // subpartitions when determining whether a tier contains the segment data.
    private final HashMap<Integer, HashSet<Integer>> subpartitionSegmentIndexes;

    private final Lock[] locks;

    private final Boolean isBroadCastOnly;

    private final int[] latestSegmentIndexes;

    public SubpartitionSegmentIndexTrackerImpl(int numSubpartitions, Boolean isBroadCastOnly) {
        int effectiveNumSubpartitions = isBroadCastOnly ? 1 : numSubpartitions;
        this.locks = new Lock[effectiveNumSubpartitions];
        this.latestSegmentIndexes = new int[numSubpartitions];
        this.isBroadCastOnly = isBroadCastOnly;
        this.subpartitionSegmentIndexes = new HashMap<>();

        Arrays.fill(latestSegmentIndexes, -1);
        for (int i = 0; i < effectiveNumSubpartitions; i++) {
            locks[i] = new ReentrantLock();
            subpartitionSegmentIndexes.put(i, new HashSet<>());
        }
    }

    // Return true if this segment tracker did not already contain the specified segment index.
    @Override
    public void addSubpartitionSegmentIndex(int subpartitionId, int segmentIndex) {
        if (latestSegmentIndexes[subpartitionId] == segmentIndex) {
            return;
        }
        latestSegmentIndexes[subpartitionId] = segmentIndex;
        int effectiveSubpartitionId = getEffectiveSubpartitionId(subpartitionId);
        callWithSubpartitionLock(
                effectiveSubpartitionId,
                () -> subpartitionSegmentIndexes.get(effectiveSubpartitionId).add(segmentIndex));
    }

    @Override
    public boolean hasCurrentSegment(int subpartitionId, int segmentIndex) {
        int effectiveSubpartitionId = getEffectiveSubpartitionId(subpartitionId);
        return callWithSubpartitionLock(
                effectiveSubpartitionId,
                () -> {
                    Set<Integer> segmentIndexes =
                            subpartitionSegmentIndexes.get(effectiveSubpartitionId);
                    if (segmentIndexes == null) {
                        return false;
                    }
                    return segmentIndexes.contains(segmentIndex);
                });
    }

    @Override
    public void release() {
        subpartitionSegmentIndexes.clear();
    }

    private int getEffectiveSubpartitionId(int subpartitionId) {
        return isBroadCastOnly ? 0 : subpartitionId;
    }

    private <R, E extends Exception> R callWithSubpartitionLock(
            int subpartitionId, SupplierWithException<R, E> callable) throws E {
        Lock lock = locks[subpartitionId];
        try {
            lock.lock();
            return callable.get();
        } finally {
            lock.unlock();
        }
    }
}
