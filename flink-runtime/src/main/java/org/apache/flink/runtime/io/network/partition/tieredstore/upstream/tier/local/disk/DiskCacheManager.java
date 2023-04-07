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

package org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.partition.tieredstore.TierType;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.BufferContext;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.CacheBufferFlushTrigger;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.CacheFlushManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.NettyBasedTierConsumerView;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierReaderViewId;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreMemoryManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.file.PartitionFileManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.file.PartitionFileType;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.file.PartitionFileWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreUtils.checkFlushCacheBuffers;

/** This class is responsible for managing cached buffers data before flush to local files. */
public class DiskCacheManager implements DiskCacheManagerOperation, CacheBufferFlushTrigger {

    private final int numSubpartitions;

    private final SubpartitionDiskCacheManager[] subpartitionDiskCacheManagers;

    private final TieredStoreMemoryManager tieredStoreMemoryManager;

    private final List<Map<TierReaderViewId, NettyBasedTierConsumerView>> tierReaderViewMap;

    private volatile CompletableFuture<Void> hasFlushCompleted =
            CompletableFuture.completedFuture(null);

    PartitionFileWriter partitionFileWriter;

    public DiskCacheManager(
            int numSubpartitions,
            int bufferSize,
            TieredStoreMemoryManager tieredStoreMemoryManager,
            CacheFlushManager cacheFlushManager,
            BufferCompressor bufferCompressor,
            PartitionFileManager partitionFileManager) {
        this.numSubpartitions = numSubpartitions;
        this.tieredStoreMemoryManager = tieredStoreMemoryManager;
        this.subpartitionDiskCacheManagers = new SubpartitionDiskCacheManager[numSubpartitions];
        this.tierReaderViewMap = new ArrayList<>(numSubpartitions);
        for (int subpartitionId = 0; subpartitionId < numSubpartitions; ++subpartitionId) {
            subpartitionDiskCacheManagers[subpartitionId] =
                    new SubpartitionDiskCacheManager(subpartitionId, bufferSize, bufferCompressor);
            tierReaderViewMap.add(new ConcurrentHashMap<>());
        }
        this.partitionFileWriter =
                partitionFileManager.createPartitionFileWriter(PartitionFileType.PRODUCER_MERGE);
        cacheFlushManager.registerCacheBufferFlushTrigger(this::flushCacheBuffers);
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
     * @param isLastBufferInSegment whether this record is the last record in a segment.
     */
    public void append(Buffer buffer, int targetChannel, boolean isLastBufferInSegment)
            throws IOException {
        try {
            getSubpartitionCacheDataManager(targetChannel).append(buffer);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
        // force spill all buffers to disk.
        if (isLastBufferInSegment) {
            flushAndReleaseCacheBuffers();
        }
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

    @Override
    public int getNumSubpartitions() {
        return numSubpartitions;
    }

    // Write lock should be acquired before invoke this method.
    @Override
    public List<BufferContext> getBuffersInOrder(int subpartitionId) {
        SubpartitionDiskCacheManager targetSubpartitionDataManager =
                getSubpartitionCacheDataManager(subpartitionId);
        return targetSubpartitionDataManager.getBuffersSatisfyStatus();
    }

    // ------------------------------------
    //      Callback for subpartition
    // ------------------------------------

    @Override
    public BufferBuilder requestBufferFromPool() throws InterruptedException {
        MemorySegment segment =
                tieredStoreMemoryManager.requestMemorySegmentBlocking(TierType.IN_DISK);
        tryCheckFlushCacheBuffers();
        return new BufferBuilder(segment, this::recycleBuffer);
    }

    private void tryCheckFlushCacheBuffers() {
        if (hasFlushCompleted.isDone()) {
            checkFlushCacheBuffers(tieredStoreMemoryManager, this);
        }
    }

    private void flushCacheBuffers() {
        checkFlushCacheBuffers(tieredStoreMemoryManager, this);
    }

    @Override
    public void onDataAvailable(
            int subpartitionId, Collection<TierReaderViewId> tierReaderViewIds) {
        Map<TierReaderViewId, NettyBasedTierConsumerView> consumerViewMap =
                tierReaderViewMap.get(subpartitionId);
        tierReaderViewIds.forEach(
                consumerId -> {
                    NettyBasedTierConsumerView consumerView = consumerViewMap.get(consumerId);
                    if (consumerView != null) {
                        consumerView.notifyDataAvailable();
                    }
                });
    }

    @Override
    public void onConsumerReleased(int subpartitionId, TierReaderViewId tierReaderViewId) {
        tierReaderViewMap.get(subpartitionId).remove(tierReaderViewId);
    }

    @Override
    public boolean isLastBufferInSegment(int subpartitionId, int bufferIndex) {
        return getSubpartitionCacheDataManager(subpartitionId)
                .getLastBufferIndexOfSegments()
                .contains(bufferIndex);
    }

    // ------------------------------------
    //           Internal Method
    // ------------------------------------

    @Override
    public void notifyFlushCachedBuffers() {
        spillBuffers(true);
    }

    private void flushAndReleaseCacheBuffers() {
        spillBuffers(false);
    }

    private synchronized void spillBuffers(boolean changeFlushState) {
        if (changeFlushState && !hasFlushCompleted.isDone()) {
            return;
        }
        List<BufferContext> bufferContexts = new ArrayList<>();
        for (int subpartitionId = 0; subpartitionId < getNumSubpartitions(); subpartitionId++) {
            bufferContexts.addAll(getBuffersInOrder(subpartitionId));
        }
        if (!bufferContexts.isEmpty()) {
            CompletableFuture<Void> spillSuccessNotifier =
                    partitionFileWriter.spillAsync(bufferContexts);
            if (changeFlushState) {
                hasFlushCompleted = spillSuccessNotifier;
            }
        }
    }

    private SubpartitionDiskCacheManager getSubpartitionCacheDataManager(int targetChannel) {
        return subpartitionDiskCacheManagers[targetChannel];
    }

    private void recycleBuffer(MemorySegment buffer) {
        tieredStoreMemoryManager.recycleBuffer(buffer, TierType.IN_DISK);
    }

    @VisibleForTesting
    public SubpartitionDiskCacheManager[] getSubpartitionDiskCacheManagers() {
        return subpartitionDiskCacheManagers;
    }
}
