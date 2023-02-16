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

package org.apache.flink.runtime.io.network.partition.store.tier.local.disk;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.partition.store.TieredStoreMode;
import org.apache.flink.runtime.io.network.partition.store.common.BufferIndexAndChannel;
import org.apache.flink.runtime.io.network.partition.store.common.BufferPoolHelper;
import org.apache.flink.runtime.io.network.partition.store.common.BufferWithIdentity;
import org.apache.flink.runtime.io.network.partition.store.common.CacheBufferSpiller;
import org.apache.flink.runtime.io.network.partition.store.common.TierReaderId;
import org.apache.flink.runtime.io.network.partition.store.common.TierReaderView;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/** This class is responsible for managing cached buffers data before flush to local files. */
public class CacheDataManager implements BufferSpillingInfoProvider, CacheDataManagerOperation {

    private static final Logger LOG = LoggerFactory.getLogger(CacheDataManager.class);

    private final int numSubpartitions;

    private final SubpartitionCacheDataManager[] subpartitionCacheDataManagers;

    private final CacheBufferSpiller spiller;

    private final TsSpillingStrategy spillStrategy;

    private final RegionBufferIndexTracker regionBufferIndexTracker;

    private final BufferPoolHelper bufferPoolHelper;

    private final AtomicInteger numUnSpillBuffers = new AtomicInteger(0);

    /**
     * Each element of the list is all views of the subpartition corresponding to its index, which
     * are stored in the form of a map that maps consumer id to its subpartition view.
     */
    private final List<Map<TierReaderId, SubpartitionConsumerInternalOperations>>
            subpartitionViewOperationsMap;

    public CacheDataManager(
            int numSubpartitions,
            int bufferSize,
            BufferPoolHelper bufferPoolHelper,
            TsSpillingStrategy spillStrategy,
            RegionBufferIndexTracker regionBufferIndexTracker,
            Path dataFilePath,
            BufferCompressor bufferCompressor)
            throws IOException {
        this.numSubpartitions = numSubpartitions;
        this.bufferPoolHelper = bufferPoolHelper;
        this.spiller = new CacheBufferLocalFileSpiller(dataFilePath);
        this.spillStrategy = spillStrategy;
        this.regionBufferIndexTracker = regionBufferIndexTracker;
        this.subpartitionCacheDataManagers = new SubpartitionCacheDataManager[numSubpartitions];

        this.subpartitionViewOperationsMap = new ArrayList<>(numSubpartitions);
        for (int subpartitionId = 0; subpartitionId < numSubpartitions; ++subpartitionId) {
            subpartitionCacheDataManagers[subpartitionId] =
                    new SubpartitionCacheDataManager(
                            subpartitionId, bufferSize, bufferCompressor, this);
            subpartitionViewOperationsMap.add(new ConcurrentHashMap<>());
        }
        bufferPoolHelper.registerSubpartitionTieredManager(
                TieredStoreMode.TieredType.IN_LOCAL, this::flushSubpartitionCachedBuffers);
    }

    // ------------------------------------
    //          For ResultPartition
    // ------------------------------------

    /**
     * Append record to {@link CacheDataManager}, It will be managed by {@link
     * SubpartitionConsumerCacheDataManager} witch it belongs to.
     *
     * @param record to be managed by this class.
     * @param targetChannel target subpartition of this record.
     * @param dataType the type of this record. In other words, is it data or event.
     * @param isLastRecordInSegment whether this record is the last record in a segment.
     */
    public void append(
            ByteBuffer record,
            int targetChannel,
            Buffer.DataType dataType,
            boolean isLastRecordInSegment)
            throws IOException {
        try {
            getSubpartitionMemoryDataManager(targetChannel)
                    .append(record, dataType, isLastRecordInSegment);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
        // force spill all buffers to disk.
        if (isLastRecordInSegment) {
            flushSubpartitionCachedBuffers();
        }
    }

    /**
     * Register {@link SubpartitionConsumerInternalOperations} to {@link
     * #subpartitionViewOperationsMap}. It is used to obtain the consumption progress of the
     * subpartition.
     */
    public TierReaderView registerNewConsumer(
            int subpartitionId,
            TierReaderId tierReaderId,
            SubpartitionConsumerInternalOperations viewOperations) {
        SubpartitionConsumerInternalOperations oldView =
                subpartitionViewOperationsMap.get(subpartitionId).put(tierReaderId, viewOperations);
        Preconditions.checkState(
                oldView == null, "Each subpartition view should have unique consumerId.");
        // return getSubpartitionMemoryDataManager(subpartitionId).registerNewConsumer(consumerId);
        return null;
    }

    /** Close this {@link CacheDataManager}, it means no data can append to memory. */
    public void close() {
        TsSpillingStrategy.Decision decision = spillStrategy.onResultPartitionClosed(this);
        LOG.debug("%%% release buffer when close");
        handleDecision(Optional.of(decision));
        spiller.close();
    }

    /**
     * Release this {@link CacheDataManager}, it means all memory taken by this class will recycle.
     */
    public void release() {
        spiller.release();
        for (int i = 0; i < numSubpartitions; i++) {
            getSubpartitionMemoryDataManager(i).release();
        }
    }

    public void setOutputMetrics(OutputMetrics metrics) {
        // HsOutputMetrics is not thread-safe. It can be shared by all the subpartitions because it
        // is expected always updated from the producer task's mailbox thread.
        for (int i = 0; i < numSubpartitions; i++) {
            getSubpartitionMemoryDataManager(i).setOutputMetrics(metrics);
        }
    }

    // ------------------------------------
    //        For Spilling Strategy
    // ------------------------------------

    @Override
    public int getPoolSize() {
        return bufferPoolHelper.numPoolSize();
    }

    @Override
    public int getNumSubpartitions() {
        return numSubpartitions;
    }

    @Override
    public int getNumTotalUnSpillBuffers() {
        return numUnSpillBuffers.get();
    }

    // Write lock should be acquired before invoke this method.
    @Override
    public Deque<BufferIndexAndChannel> getBuffersInOrder(
            int subpartitionId, SpillStatus spillStatus, ConsumeStatusWithId consumeStatusWithId) {
        SubpartitionCacheDataManager targetSubpartitionDataManager =
                getSubpartitionMemoryDataManager(subpartitionId);
        return targetSubpartitionDataManager.getBuffersSatisfyStatus(
                spillStatus, consumeStatusWithId);
    }

    // Write lock should be acquired before invoke this method.
    @Override
    public List<Integer> getNextBufferIndexToConsume(TierReaderId tierReaderId) {
        ArrayList<Integer> consumeIndexes = new ArrayList<>(numSubpartitions);
        for (int channel = 0; channel < numSubpartitions; channel++) {
            SubpartitionConsumerInternalOperations viewOperation =
                    subpartitionViewOperationsMap.get(channel).get(tierReaderId);
            // Access consuming offset without lock to prevent deadlock.
            // A consuming thread may being blocked on the memory data manager lock, while holding
            // the viewOperation lock.
            consumeIndexes.add(
                    viewOperation == null ? -1 : viewOperation.getConsumingOffset(false) + 1);
        }
        return consumeIndexes;
    }

    // ------------------------------------
    //      Callback for subpartition
    // ------------------------------------

    @Override
    public void markBufferReleasedFromFile(int subpartitionId, int bufferIndex) {
        regionBufferIndexTracker.markBufferReleased(subpartitionId, bufferIndex);
    }

    @Override
    public BufferBuilder requestBufferFromPool() throws InterruptedException {
        MemorySegment segment =
                bufferPoolHelper.requestMemorySegmentBlocking(
                        TieredStoreMode.TieredType.IN_LOCAL, false);
        return new BufferBuilder(segment, this::recycleBuffer);
    }

    @Override
    public void onBufferConsumed(BufferIndexAndChannel consumedBuffer) {
        Optional<TsSpillingStrategy.Decision> decision =
                spillStrategy.onBufferConsumed(consumedBuffer);
        LOG.debug("%%% release buffer when onBufferConsumed");
        handleDecision(decision);
    }

    @Override
    public void onBufferFinished() {
        LOG.debug("%%% release buffer when buffer finished..");
        Optional<TsSpillingStrategy.Decision> decision =
                spillStrategy.onBufferFinished(numUnSpillBuffers.incrementAndGet(), getPoolSize());
        handleDecision(decision);
        bufferPoolHelper.checkNeedFlushCachedBuffers();
    }

    @Override
    public void onDataAvailable(int subpartitionId, Collection<TierReaderId> tierReaderIds) {
        Map<TierReaderId, SubpartitionConsumerInternalOperations> consumerViewMap =
                subpartitionViewOperationsMap.get(subpartitionId);
        tierReaderIds.forEach(
                consumerId -> {
                    SubpartitionConsumerInternalOperations consumerView =
                            consumerViewMap.get(consumerId);
                    if (consumerView != null) {
                        consumerView.notifyDataAvailable();
                    }
                });
    }

    @Override
    public void onConsumerReleased(int subpartitionId, TierReaderId tierReaderId) {
        subpartitionViewOperationsMap.get(subpartitionId).remove(tierReaderId);
        // getSubpartitionMemoryDataManager(subpartitionId).releaseConsumer(consumerId);
    }

    @Override
    public boolean isLastBufferInSegment(int subpartitionId, int bufferIndex) {
        return getSubpartitionMemoryDataManager(subpartitionId)
                .getLastBufferIndexOfSegments()
                .contains(bufferIndex);
    }

    // ------------------------------------
    //           Internal Method
    // ------------------------------------

    // Attention: Do not call this method within the read lock and subpartition lock, otherwise
    // deadlock may occur as this method maybe acquire write lock and other subpartition's lock
    // inside.
    private void handleDecision(
            @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
                    Optional<TsSpillingStrategy.Decision> decisionOpt) {
        if (decisionOpt.isPresent()) {
            TsSpillingStrategy.Decision decision = decisionOpt.get();
            spillBuffers(decision.getBufferToSpill());
            releaseBuffers(decision.getBufferToRelease());
        }
    }

    private void flushSubpartitionCachedBuffers() {
        LOG.debug("%%% release buffer when flushSubpartitionCachedBuffers");
        TsSpillingStrategy.Decision decision = spillStrategy.forceTriggerFlushCachedBuffers(this);
        spillBuffers(decision.getBufferToSpill());
        releaseBuffers(decision.getBufferToRelease());
    }

    /**
     * Spill buffers for each subpartition in a decision.
     *
     * <p>Note that: The method should not be locked, it is the responsibility of each subpartition
     * to maintain thread safety itself.
     *
     * @param toSpill All buffers that need to be spilled in a decision.
     */
    private void spillBuffers(Map<Integer, List<BufferIndexAndChannel>> toSpill) {
        if (toSpill.isEmpty()) {
            return;
        }
        CompletableFuture<Void> spillingCompleteFuture = new CompletableFuture<>();
        List<BufferWithIdentity> bufferWithIdentities = new ArrayList<>();
        toSpill.forEach(
                (subpartitionId, bufferIndexAndChannels) -> {
                    SubpartitionCacheDataManager subpartitionDataManager =
                            getSubpartitionMemoryDataManager(subpartitionId);
                    bufferWithIdentities.addAll(
                            subpartitionDataManager.spillSubpartitionBuffers(
                                    bufferIndexAndChannels, spillingCompleteFuture));
                    // decrease numUnSpillBuffers as this subpartition's buffer is spill.
                    numUnSpillBuffers.getAndAdd(-bufferIndexAndChannels.size());
                });
        if (!bufferWithIdentities.isEmpty()) {
            FutureUtils.assertNoException(
                    spiller.spillAsync(bufferWithIdentities)
                            .thenAccept(
                                    spilledBuffers -> {
                                        regionBufferIndexTracker.addBuffers(spilledBuffers);
                                        spillingCompleteFuture.complete(null);
                                    }));
        }
    }

    /**
     * Release buffers for each subpartition in a decision.
     *
     * <p>Note that: The method should not be locked, it is the responsibility of each subpartition
     * to maintain thread safety itself.
     *
     * @param toRelease All buffers that need to be released in a decision.
     */
    private void releaseBuffers(Map<Integer, List<BufferIndexAndChannel>> toRelease) {
        if (toRelease.isEmpty()) {
            return;
        }
        toRelease.forEach(
                (subpartitionId, subpartitionBuffers) ->
                        getSubpartitionMemoryDataManager(subpartitionId)
                                .releaseSubpartitionBuffers(subpartitionBuffers));
    }

    private SubpartitionCacheDataManager getSubpartitionMemoryDataManager(int targetChannel) {
        return subpartitionCacheDataManagers[targetChannel];
    }

    private void recycleBuffer(MemorySegment buffer) {
        bufferPoolHelper.recycleBuffer(buffer, TieredStoreMode.TieredType.IN_LOCAL, false);
    }
}
