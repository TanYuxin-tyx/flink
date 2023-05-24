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
 * The implementation of {@link SubpartitionSegmentIdTracker}. Each {@link TierProducerAgent} has a
 * {@link SubpartitionSegmentIdTrackerImpl} to record the segments belong to the producer agent.
 */
public class SubpartitionSegmentIdTrackerImpl implements SubpartitionSegmentIdTracker {

    /**
     * Each subpartition calculates the amount of data written to a tier separately. If the amount
     * of data exceeds the threshold, the current writing segment will be finished. Because
     * different subpartitions may have duplicate segment indexes, this field needs to distinguish
     * between different subpartitions.
     */
    private final Map<TieredStorageSubpartitionId, HashSet<Integer>> subpartitionSegmentIds;

    /** The locks for all subpartitions. */
    private final Lock[] locks;

    /** Indicate whether this is a broadcast only partition. */
    private final Boolean isBroadCastOnly;

    /** Record the latest segment index for each subpartition. */
    private final int[] latestSegmentIds;

    public SubpartitionSegmentIdTrackerImpl(int numSubpartitions, Boolean isBroadcastOnly) {
        this.isBroadCastOnly = isBroadcastOnly;
        int effectiveNumSubpartitions = isBroadcastOnly ? 1 : numSubpartitions;
        this.locks = new Lock[effectiveNumSubpartitions];
        this.latestSegmentIds = new int[effectiveNumSubpartitions];
        this.subpartitionSegmentIds = new HashMap<>();

        Arrays.fill(latestSegmentIds, -1);
        for (int i = 0; i < effectiveNumSubpartitions; i++) {
            locks[i] = new ReentrantLock();
            subpartitionSegmentIds.put(new TieredStorageSubpartitionId(i), new HashSet<>());
        }
    }

    @Override
    public void addSubpartitionSegmentIndex(
            TieredStorageSubpartitionId subpartitionId, int segmentId) {
        if (latestSegmentIds[subpartitionId.getSubpartitionId()] == segmentId) {
            return;
        }
        latestSegmentIds[subpartitionId.getSubpartitionId()] = segmentId;
        TieredStorageSubpartitionId effectiveSubpartitionId =
                getEffectiveSubpartitionId(subpartitionId);
        callWithSubpartitionLock(
                effectiveSubpartitionId.getSubpartitionId(),
                () -> subpartitionSegmentIds.get(effectiveSubpartitionId).add(segmentId));
    }

    @Override
    public boolean hasCurrentSegment(TieredStorageSubpartitionId subpartitionId, int segmentId) {
        TieredStorageSubpartitionId effectiveSubpartitionId =
                getEffectiveSubpartitionId(subpartitionId);
        return callWithSubpartitionLock(
                effectiveSubpartitionId.getSubpartitionId(),
                () -> {
                    Set<Integer> segmentIndexes =
                            subpartitionSegmentIds.get(effectiveSubpartitionId);
                    if (segmentIndexes == null) {
                        return false;
                    }
                    return segmentIndexes.contains(segmentId);
                });
    }

    @Override
    public void release() {
        subpartitionSegmentIds.clear();
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
