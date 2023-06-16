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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.disk;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyPayload;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file.PartitionFileWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file.SegmentNettyPayload;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file.SubpartitionNettyPayload;
import org.apache.flink.util.concurrent.FutureUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * The {@link DiskCacheManager} is responsible for managing cached buffers before flushing to files.
 */
public class DiskCacheManager {

    private final int numSubpartitions;

    private final PartitionFileWriter partitionFileWriter;

    private final SubpartitionDiskCacheManager[] subpartitionCacheManagers;

    private CompletableFuture<Void> hasFlushCompleted;

    public DiskCacheManager(
            int numSubpartitions,
            TieredStorageMemoryManager storageMemoryManager,
            PartitionFileWriter partitionFileWriter) {
        this.numSubpartitions = numSubpartitions;
        this.partitionFileWriter = partitionFileWriter;
        this.subpartitionCacheManagers = new SubpartitionDiskCacheManager[numSubpartitions];
        this.hasFlushCompleted = FutureUtils.completedVoidFuture();

        for (int subpartitionId = 0; subpartitionId < numSubpartitions; ++subpartitionId) {
            subpartitionCacheManagers[subpartitionId] =
                    new SubpartitionDiskCacheManager(subpartitionId);
        }
        storageMemoryManager.listenBufferReclaimRequest(this::notifyFlushCachedBuffers);
    }

    /**
     * Append the end-of-segment event to {@link DiskCacheManager}.
     *
     * @param record the end-of-segment event
     * @param subpartitionId target subpartition of this record.
     * @param dataType the type of this record. In other words, is it data or event.
     */
    public void appendEndOfSegmentEvent(
            ByteBuffer record, int subpartitionId, Buffer.DataType dataType) {
        subpartitionCacheManagers[subpartitionId].appendEndOfSegmentEvent(record, dataType);
        flushAndReleaseCacheBuffers();
    }

    /**
     * Append buffer to {@link DiskCacheManager}.
     *
     * @param buffer to be managed by this class.
     * @param subpartitionId the subpartition of this record.
     */
    public void append(Buffer buffer, int subpartitionId) {
        subpartitionCacheManagers[subpartitionId].append(buffer);
    }

    /**
     * Return the finished buffer index.
     *
     * @param subpartitionId the target subpartition id
     * @return the finished buffer index
     */
    public int getFinishedBufferIndex(int subpartitionId) {
        return subpartitionCacheManagers[subpartitionId].getFinishedBufferIndex();
    }

    /** Close this {@link DiskCacheManager}, it means no data can append to memory. */
    public void close() {
        flushAndReleaseCacheBuffers();
    }

    /**
     * Release this {@link DiskCacheManager}, it means all memory taken by this class will recycle.
     */
    public void release() {
        Arrays.stream(subpartitionCacheManagers).forEach(SubpartitionDiskCacheManager::release);
        partitionFileWriter.release();
    }

    // ------------------------------------
    //           Internal Method
    // ------------------------------------

    private void notifyFlushCachedBuffers() {
        flushBuffers(false);
    }

    private void flushAndReleaseCacheBuffers() {
        flushBuffers(true);
    }

    /**
     * Note that the request of flushing buffers may come from the disk check thread or the task
     * thread, so the method itself should ensure the thread safety.
     */
    private synchronized void flushBuffers(boolean needForceFlush) {
        if (!needForceFlush && !hasFlushCompleted.isDone()) {
            return;
        }
        List<SubpartitionNettyPayload> toWriteBuffers = new ArrayList<>();
        int numToWriteBuffers = getSubpartitionBuffersToFlush(toWriteBuffers);

        if (numToWriteBuffers > 0) {
            CompletableFuture<Void> flushCompletableFuture =
                    partitionFileWriter.write(toWriteBuffers);
            if (!needForceFlush) {
                hasFlushCompleted = flushCompletableFuture;
            }
        }
    }

    private int getSubpartitionBuffersToFlush(List<SubpartitionNettyPayload> toWriteBuffers) {
        int numToWriteBuffers = 0;
        for (int subpartitionId = 0; subpartitionId < numSubpartitions; subpartitionId++) {
            List<NettyPayload> nettyPayloads =
                    subpartitionCacheManagers[subpartitionId].getAllBuffers();
            toWriteBuffers.add(
                    new SubpartitionNettyPayload(
                            subpartitionId,
                            Collections.singletonList(
                                    new SegmentNettyPayload(-1, nettyPayloads, false))));
            numToWriteBuffers += nettyPayloads.size();
        }
        return numToWriteBuffers;
    }
}
