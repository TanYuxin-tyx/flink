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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.remote;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.PartitionFileWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkState;

/** This class is responsible for managing the data in a single subpartition. */
public class SubpartitionRemoteCacheManager {

    private static final Logger LOG = LoggerFactory.getLogger(SubpartitionRemoteCacheManager.class);

    private final TieredStoragePartitionId partitionId;

    private final int subpartitionId;

    private final PartitionFileWriter partitionFileWriter;

    private final Deque<Tuple2<Buffer, Integer>> allBuffers = new LinkedList<>();

    private CompletableFuture<Void> hasSpillCompleted = FutureUtils.completedVoidFuture();

    private int bufferIndex;

    private int segmentIndex = -1;

    public SubpartitionRemoteCacheManager(
            TieredStoragePartitionId partitionId,
            int subpartitionId,
            TieredStorageMemoryManager storageMemoryManager,
            PartitionFileWriter partitionFileWriter) {
        this.partitionId = partitionId;
        this.subpartitionId = subpartitionId;
        this.partitionFileWriter = partitionFileWriter;
        storageMemoryManager.listenBufferReclaimRequest(this::spillBuffers);
    }

    // ------------------------------------------------------------------------
    //  Called by RemoteCacheManager
    // ------------------------------------------------------------------------

    void startSegment(int segmentIndex) {
        this.segmentIndex = segmentIndex;
    }

    void addBuffer(Buffer buffer) {
        Tuple2<Buffer, Integer> toAddBuffer = new Tuple2<>(buffer, bufferIndex);
        synchronized (allBuffers) {
            bufferIndex++;
            allBuffers.add(toAddBuffer);
        }
    }

    void finishSegment(int segmentIndex) {
        checkState(this.segmentIndex == segmentIndex, "Wrong segment index.");
        int bufferNumber = spillBuffers();
        if (bufferNumber > 0) {
            PartitionFileWriter.SubpartitionBufferContext finishSegmentBuffer =
                    new PartitionFileWriter.SubpartitionBufferContext(
                            subpartitionId,
                            Collections.singletonList(
                                    new PartitionFileWriter.SegmentBufferContext(
                                            segmentIndex, Collections.emptyList(), true)));
            hasSpillCompleted =
                    partitionFileWriter.write(
                            partitionId, Collections.singletonList(finishSegmentBuffer));
        }
        checkState(allBuffers.isEmpty(), "Leaking buffers.");
    }

    void close() {
        // Wait the spilling buffers to be completed before closed
        try {
            hasSpillCompleted.get();
        } catch (Exception e) {
            LOG.error("Failed to spill the buffers.", e);
            ExceptionUtils.rethrow(e);
        }
        spillBuffers();
    }

    /** Release all buffers. */
    void release() {
        checkState(
                hasSpillCompleted.isDone() || hasSpillCompleted.isCancelled(),
                "Uncompleted spilling buffers.");
        recycleBuffers();
        checkState(allBuffers.isEmpty(), "Leaking buffers.");
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    private int spillBuffers() {
        synchronized (allBuffers) {
            List<Tuple2<Buffer, Integer>> allBuffersToFlush = new ArrayList<>(allBuffers);
            allBuffers.clear();
            if (allBuffersToFlush.isEmpty()) {
                return 0;
            }

            PartitionFileWriter.SubpartitionBufferContext subpartitionSpilledBuffers =
                    new PartitionFileWriter.SubpartitionBufferContext(
                            subpartitionId,
                            Collections.singletonList(
                                    new PartitionFileWriter.SegmentBufferContext(
                                            segmentIndex, allBuffersToFlush, false)));
            hasSpillCompleted =
                    partitionFileWriter.write(
                            partitionId, Collections.singletonList(subpartitionSpilledBuffers));
            return allBuffersToFlush.size();
        }
    }

    private void recycleBuffers() {
        synchronized (allBuffers) {
            for (Tuple2<Buffer, Integer> bufferAndIndex : allBuffers) {
                Buffer buffer = bufferAndIndex.f0;
                if (buffer.isRecycled()) {
                    buffer.recycleBuffer();
                }
            }
            allBuffers.clear();
        }
    }
}
