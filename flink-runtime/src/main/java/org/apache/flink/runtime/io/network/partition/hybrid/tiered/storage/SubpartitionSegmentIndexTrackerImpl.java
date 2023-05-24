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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage;

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierProducerAgent;
import org.apache.flink.util.function.SupplierWithException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The implementation of {@link SubpartitionSegmentIndexTracker}. Each {@link TierProducerAgent}'s
 * data manager has a separate {@link SubpartitionSegmentIndexTrackerImpl}.
 */
public class SubpartitionSegmentIndexTrackerImpl implements SubpartitionSegmentIndexTracker {

    /**
     * Each subpartition calculates the amount of data written to a tier separately. If the amount
     * of data exceeds the threshold, the current writing segment will be finished. Because
     * different subpartitions may have duplicate segment indexes, this field needs to distinguish
     * between different subpartitions.
     */
    private final Map<TieredStorageSubpartitionId, HashSet<Integer>> subpartitionSegmentIndexes;

    /** The locks for all subpartitions. */
    private final Lock[] locks;

    /** Indicate whether this is a broadcast only partition. */
    private final Boolean isBroadCastOnly;

    /** Record the latest segment index for each subpartition. */
    private final int[] latestSegmentIndexes;

    public SubpartitionSegmentIndexTrackerImpl(int numSubpartitions, Boolean isBroadcastOnly) {
        this.isBroadCastOnly = isBroadcastOnly;
        int effectiveNumSubpartitions = isBroadcastOnly ? 1 : numSubpartitions;
        this.locks = new Lock[effectiveNumSubpartitions];
        this.latestSegmentIndexes = new int[numSubpartitions];
        this.subpartitionSegmentIndexes = new HashMap<>();

        Arrays.fill(latestSegmentIndexes, -1);
        for (int i = 0; i < effectiveNumSubpartitions; i++) {
            locks[i] = new ReentrantLock();
            subpartitionSegmentIndexes.put(new TieredStorageSubpartitionId(i), new HashSet<>());
        }
    }

    @Override
    public void addSubpartitionSegmentIndex(
            TieredStorageSubpartitionId subpartitionId, int segmentIndex) {
        if (latestSegmentIndexes[subpartitionId.getSubpartitionId()] == segmentIndex) {
            return;
        }
        latestSegmentIndexes[subpartitionId.getSubpartitionId()] = segmentIndex;
        TieredStorageSubpartitionId effectiveSubpartitionId =
                getEffectiveSubpartitionId(subpartitionId);
        callWithSubpartitionLock(
                effectiveSubpartitionId.getSubpartitionId(),
                () -> subpartitionSegmentIndexes.get(effectiveSubpartitionId).add(segmentIndex));
    }

    @Override
    public boolean hasCurrentSegment(TieredStorageSubpartitionId subpartitionId, int segmentIndex) {
        TieredStorageSubpartitionId effectiveSubpartitionId =
                getEffectiveSubpartitionId(subpartitionId);
        return callWithSubpartitionLock(
                effectiveSubpartitionId.getSubpartitionId(),
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

    private TieredStorageSubpartitionId getEffectiveSubpartitionId(
            TieredStorageSubpartitionId subpartitionId) {
        return isBroadCastOnly ? new TieredStorageSubpartitionId(0) : subpartitionId;
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
