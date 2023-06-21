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

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.PartitionFileWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyPayload;
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

import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageUtils.convertToSpilledBufferContext;
import static org.apache.flink.util.Preconditions.checkState;

/** This class is responsible for managing the data in a single subpartition. */
public class SubpartitionRemoteCacheManager {

    private static final Logger LOG = LoggerFactory.getLogger(SubpartitionRemoteCacheManager.class);

    private final int subpartitionId;

    private final PartitionFileWriter partitionFileWriter;

    private final Deque<NettyPayload> allBuffers = new LinkedList<>();

    private CompletableFuture<Void> hasFlushCompleted = FutureUtils.completedVoidFuture();

    private int bufferIndex;

    private int segmentIndex = -1;

    public SubpartitionRemoteCacheManager(
            int subpartitionId,
            TieredStorageMemoryManager storageMemoryManager,
            PartitionFileWriter partitionFileWriter) {
        this.subpartitionId = subpartitionId;
        this.partitionFileWriter = partitionFileWriter;
        storageMemoryManager.listenBufferReclaimRequest(this::flushBuffers);
    }

    // ------------------------------------------------------------------------
    //  Called by RemoteCacheManager
    // ------------------------------------------------------------------------

    void startSegment(int segmentIndex) {
        this.segmentIndex = segmentIndex;
    }

    void addBuffer(Buffer buffer) {
        NettyPayload toAddBuffer = NettyPayload.newBuffer(buffer, bufferIndex, subpartitionId);
        synchronized (allBuffers) {
            bufferIndex++;
            allBuffers.add(toAddBuffer);
        }
    }

    void finishSegment(int segmentIndex) {
        checkState(this.segmentIndex == segmentIndex);
        int bufferNumber = flushBuffers();
        if (bufferNumber > 0) {
            PartitionFileWriter.SubpartitionSpilledBufferContext finishSegmentBuffer =
                    new PartitionFileWriter.SubpartitionSpilledBufferContext(
                            subpartitionId,
                            Collections.singletonList(
                                    new PartitionFileWriter.SegmentSpilledBufferContext(
                                            segmentIndex, Collections.emptyList(), true)));
            hasFlushCompleted =
                    partitionFileWriter.write(Collections.singletonList(finishSegmentBuffer));
        }
        checkState(allBuffers.isEmpty(), "Leaking buffers.");
    }

    void close() {
        try {
            hasFlushCompleted.get();
        } catch (Exception e) {
            LOG.error("Failed to finish the spilling process.", e);
            ExceptionUtils.rethrow(e);
        }
        flushBuffers();
    }

    /** Release all buffers. */
    void release() {
        // Wait the flushing buffers to be completed before released
        try {
            hasFlushCompleted.get();
        } catch (Exception e) {
            LOG.error("Failed to flush the buffers.", e);
            ExceptionUtils.rethrow(e);
        }

        checkState(allBuffers.isEmpty(), "Leaking buffers.");
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    private int flushBuffers() {
        synchronized (allBuffers) {
            List<NettyPayload> allBuffersToFlush = new ArrayList<>(allBuffers);
            allBuffers.clear();
            if (allBuffersToFlush.isEmpty()) {
                return 0;
            }

            PartitionFileWriter.SubpartitionSpilledBufferContext subpartitionSpilledBuffers =
                    new PartitionFileWriter.SubpartitionSpilledBufferContext(
                            subpartitionId,
                            Collections.singletonList(
                                    new PartitionFileWriter.SegmentSpilledBufferContext(
                                            segmentIndex,
                                            convertToSpilledBufferContext(allBuffersToFlush),
                                            false)));
            hasFlushCompleted =
                    partitionFileWriter.write(
                            Collections.singletonList(subpartitionSpilledBuffers));
            return allBuffersToFlush.size();
        }
    }
}
