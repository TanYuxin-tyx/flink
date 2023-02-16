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

package org.apache.flink.runtime.io.network.partition.store.common;

import org.apache.flink.util.function.SupplierWithException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * {@link SubpartitionSegmentIndexTracker} is to track segment index for each subpartition. Each
 * {@link StorageTier}'s data manager has a separate {@link SubpartitionSegmentIndexTracker}.
 */
public class SubpartitionSegmentIndexTracker {

    // Each subpartition calculates the amount of data written to a tier separately. If the
    // amount of data exceeds the threshold, the segment is switched. Different subpartitions
    // may have duplicate segment indexes, so it is necessary to distinguish different
    // subpartitions when determining whether a tier contains the segment data.
    private final HashMap<Integer, HashSet<Long>> subpartitionSegmentIndexes;

    private final Lock[] locks;

    private final Boolean isBroadCastOnly;

    public SubpartitionSegmentIndexTracker(int numSubpartitions, Boolean isBroadCastOnly) {
        int numSubpartitions1 = isBroadCastOnly ? 1 : numSubpartitions;
        this.locks = new Lock[numSubpartitions1];
        this.subpartitionSegmentIndexes = new HashMap<>();
        for (int i = 0; i < numSubpartitions1; i++) {
            locks[i] = new ReentrantLock();
            subpartitionSegmentIndexes.put(i, new HashSet<>());
        }
        this.isBroadCastOnly = isBroadCastOnly;
    }

    // Return true if this segment tracker did not already contain the specified segment index.
    public boolean addSubpartitionSegmentIndex(int subpartitionId, long segmentIndex) {
        if (isBroadCastOnly) {
            return callWithSubpartitionLock(
                    0, () -> subpartitionSegmentIndexes.get(0).add(segmentIndex));
        } else {
            return callWithSubpartitionLock(
                    subpartitionId,
                    () -> subpartitionSegmentIndexes.get(subpartitionId).add(segmentIndex));
        }
    }

    public boolean hasCurrentSegment(int subpartitionId, long segmentIndex) {
        if (isBroadCastOnly) {
            return callWithSubpartitionLock(
                    0,
                    () -> {
                        Set<Long> segmentIndexes = subpartitionSegmentIndexes.get(0);
                        if (segmentIndexes == null) {
                            return false;
                        }
                        return segmentIndexes.contains(segmentIndex);
                    });
        } else {
            return callWithSubpartitionLock(
                    subpartitionId,
                    () -> {
                        Set<Long> segmentIndexes;
                        segmentIndexes = subpartitionSegmentIndexes.get(subpartitionId);
                        if (segmentIndexes == null) {
                            return false;
                        }
                        return segmentIndexes.contains(segmentIndex);
                    });
        }
    }

    public void release() {
        subpartitionSegmentIndexes.clear();
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
