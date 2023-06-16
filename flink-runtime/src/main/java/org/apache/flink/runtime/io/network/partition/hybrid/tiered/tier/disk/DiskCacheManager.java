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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** This class is responsible for managing cached buffers data before flush to local files. */
public class DiskCacheManager {

    private final int numSubpartitions;

    private final SubpartitionDiskCacheManager[] subpartitionDiskCacheManagers;

    private volatile CompletableFuture<Void> hasFlushCompleted =
            CompletableFuture.completedFuture(null);

    PartitionFileWriter partitionFileWriter;

    public DiskCacheManager(
            int numSubpartitions,
            TieredStorageMemoryManager storageMemoryManager,
            PartitionFileWriter partitionFileWriter) {
        this.numSubpartitions = numSubpartitions;
        this.subpartitionDiskCacheManagers = new SubpartitionDiskCacheManager[numSubpartitions];
        for (int subpartitionId = 0; subpartitionId < numSubpartitions; ++subpartitionId) {
            subpartitionDiskCacheManagers[subpartitionId] =
                    new SubpartitionDiskCacheManager(subpartitionId);
        }
        this.partitionFileWriter = partitionFileWriter;
        storageMemoryManager.listenBufferReclaimRequest(this::notifyFlushCachedBuffers);
    }

    // ------------------------------------
    //          For ResultPartition
    // ------------------------------------

    /**
     * Append record to {@link DiskCacheManager}.
     *
     * @param record to be managed by this class.
     * @param targetChannel target subpartition of this record.
     * @param dataType the type of this record. In other words, is it data or event.
     */
    public void appendSegmentEvent(ByteBuffer record, int targetChannel, Buffer.DataType dataType) {
        getSubpartitionCacheDataManager(targetChannel).appendSegmentEvent(record, dataType);
        flushAndReleaseCacheBuffers();
    }

    /**
     * Append buffer to {@link DiskCacheManager}.
     *
     * @param buffer to be managed by this class.
     * @param targetChannel target subpartition of this record.
     */
    public void append(Buffer buffer, int targetChannel) {
        getSubpartitionCacheDataManager(targetChannel).append(buffer);
    }

    /** Close this {@link DiskCacheManager}, it means no data can append to memory. */
    public void close() {
        flushAndReleaseCacheBuffers();
    }

    /**
     * Release this {@link DiskCacheManager}, it means all memory taken by this class will recycle.
     */
    public void release() {
        for (int i = 0; i < numSubpartitions; i++) {
            getSubpartitionCacheDataManager(i).release();
        }
        partitionFileWriter.release();
    }

    // ------------------------------------
    //        For Spilling Strategy
    // ------------------------------------

    public int getFinishedBufferIndex(int subpartitionId) {
        return subpartitionDiskCacheManagers[subpartitionId].getFinishedBufferIndex();
    }

    public List<NettyPayload> getBuffersInOrder(int subpartitionId) {
        SubpartitionDiskCacheManager targetSubpartitionDataManager =
                getSubpartitionCacheDataManager(subpartitionId);
        return targetSubpartitionDataManager.getBuffersSatisfyStatus();
    }

    // ------------------------------------
    //      Callback for subpartition
    // ------------------------------------

    // ------------------------------------
    //           Internal Method
    // ------------------------------------

    private void notifyFlushCachedBuffers() {
        spillBuffers(true);
    }

    private void flushAndReleaseCacheBuffers() {
        spillBuffers(false);
    }

    private synchronized void spillBuffers(boolean changeFlushState) {
        if (changeFlushState && !hasFlushCompleted.isDone()) {
            return;
        }
        List<SubpartitionNettyPayload> toWriteBuffers = new ArrayList<>();
        int numToWriteBuffers = 0;
        for (int subpartitionId = 0; subpartitionId < numSubpartitions; subpartitionId++) {
            List<NettyPayload> nettyPayloads = getBuffersInOrder(subpartitionId);
            toWriteBuffers.add(
                    new SubpartitionNettyPayload(
                            subpartitionId,
                            Collections.singletonList(
                                    new SegmentNettyPayload(-1, nettyPayloads, false))));
            numToWriteBuffers += nettyPayloads.size();
        }

        if (numToWriteBuffers > 0) {
            CompletableFuture<Void> spillSuccessNotifier =
                    partitionFileWriter.write(toWriteBuffers);
            if (changeFlushState) {
                hasFlushCompleted = spillSuccessNotifier;
            }
        }
    }

    private SubpartitionDiskCacheManager getSubpartitionCacheDataManager(int targetChannel) {
        return subpartitionDiskCacheManagers[targetChannel];
    }
}
