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
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file.PartitionFileManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file.PartitionFileType;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file.PartitionFileWriter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** This class is responsible for managing cached buffers data before flush to local files. */
public class DiskCacheManager implements DiskCacheManagerOperation {

    private final int numSubpartitions;

    private final SubpartitionDiskCacheManager[] subpartitionDiskCacheManagers;

    private volatile CompletableFuture<Void> hasFlushCompleted =
            CompletableFuture.completedFuture(null);

    PartitionFileWriter partitionFileWriter;

    public DiskCacheManager(
            int numSubpartitions,
            TieredStorageMemoryManager storageMemoryManager,
            PartitionFileManager partitionFileManager) {
        this.numSubpartitions = numSubpartitions;
        this.subpartitionDiskCacheManagers = new SubpartitionDiskCacheManager[numSubpartitions];
        for (int subpartitionId = 0; subpartitionId < numSubpartitions; ++subpartitionId) {
            subpartitionDiskCacheManagers[subpartitionId] =
                    new SubpartitionDiskCacheManager(subpartitionId);
        }
        this.partitionFileWriter =
                partitionFileManager.createPartitionFileWriter(PartitionFileType.PRODUCER_MERGE);
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

    @Override
    public int getNumSubpartitions() {
        return numSubpartitions;
    }

    // Write lock should be acquired before invoke this method.
    @Override
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
        List<NettyPayload> nettyPayloads = new ArrayList<>();
        for (int subpartitionId = 0; subpartitionId < getNumSubpartitions(); subpartitionId++) {
            nettyPayloads.addAll(getBuffersInOrder(subpartitionId));
        }
        if (!nettyPayloads.isEmpty()) {
            CompletableFuture<Void> spillSuccessNotifier =
                    partitionFileWriter.spillAsync(-1, -1, nettyPayloads);
            if (changeFlushState) {
                hasFlushCompleted = spillSuccessNotifier;
            }
        }
    }

    private SubpartitionDiskCacheManager getSubpartitionCacheDataManager(int targetChannel) {
        return subpartitionDiskCacheManagers[targetChannel];
    }
}
