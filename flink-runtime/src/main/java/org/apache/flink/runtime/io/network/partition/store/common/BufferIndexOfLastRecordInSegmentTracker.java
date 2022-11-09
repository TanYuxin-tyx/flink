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
 * {@link BufferIndexOfLastRecordInSegmentTracker} is to track buffer index for each subpartition.
 */
public class BufferIndexOfLastRecordInSegmentTracker {

    private HashMap<Integer, HashSet<Integer>> subpartitionBufferIndexes;

    private final Lock[] locks;

    public BufferIndexOfLastRecordInSegmentTracker(int numSubpartitions) {
        this.locks = new Lock[numSubpartitions];
        this.subpartitionBufferIndexes = new HashMap<>();
        for (int i = 0; i < numSubpartitions; i++) {
            locks[i] = new ReentrantLock();
            subpartitionBufferIndexes.put(i, new HashSet<>());
        }
    }

    public boolean addBufferIndexOfLastRecordInSegment(int subpartitionId, int bufferIndex) {
        return callWithSubpartitionLock(
                subpartitionId,
                () -> subpartitionBufferIndexes.get(subpartitionId).add(bufferIndex));
    }

    public boolean isLastRecordInSegment(int subpartitionId, int bufferIndex) {
        return callWithSubpartitionLock(
                subpartitionId,
                () -> {
                    Set<Integer> segmentIndexes = subpartitionBufferIndexes.get(subpartitionId);
                    if (segmentIndexes == null) {
                        return false;
                    }
                    return segmentIndexes.contains(bufferIndex);
                });
    }

    public void release() {
        subpartitionBufferIndexes = null;
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
