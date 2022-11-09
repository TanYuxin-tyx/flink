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

package org.apache.flink.runtime.io.network.partition.tieredstore.upstream;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ChannelStateHolder;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.CacheFlushManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.OutputMetrics;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.StorageTier;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreMemoryManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreProducer;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.UpstreamTieredStoreMemoryManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.file.PartitionFileManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.file.PartitionFileManagerImpl;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.service.TieredStoreNettyService;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.service.TieredStoreNettyServiceImpl;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.DiskTier;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.RegionBufferIndexTrackerImpl;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.memory.MemoryTier;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.remote.RemoteTier;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.HYBRID_SELECTIVE;
import static org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreUtils.DATA_FILE_SUFFIX;
import static org.apache.flink.runtime.shuffle.NettyShuffleUtils.HYBRID_SHUFFLE_TIER_EXCLUSIVE_BUFFERS;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link TieredStoreResultPartition} appends records and events to the tiered store, which supports
 * the upstream dynamically switches storage tier for writing shuffle data, and the downstream will
 * read data from the relevant storage tier.
 */
public class TieredStoreResultPartition extends ResultPartition implements ChannelStateHolder {

    public final Map<TieredStoreMode.TierType, Integer> tierExclusiveBuffers;

    private final int networkBufferSize;

    private final TieredStoreConfiguration storeConfiguration;

    private final String dataFileBasePath;

    private final float minReservedDiskSpaceFraction;

    private final float numBuffersTriggerFlushRatio;

    private final boolean isBroadcast;

    private final CacheFlushManager cacheFlushManager;

    private final PartitionFileManager partitionFileManager;

    private StorageTier[] allTiers;

    private TieredStoreProducer tieredStoreProducer;

    private boolean hasNotifiedEndOfUserRecords;

    private TieredStoreMemoryManager tieredStoreMemoryManager;

    private TieredStoreNettyService tieredStoreNettyService;

    public TieredStoreResultPartition(
            JobID jobID,
            String owningTaskName,
            int partitionIndex,
            ResultPartitionID partitionId,
            ResultPartitionType partitionType,
            int numSubpartitions,
            int numTargetKeyGroups,
            BatchShuffleReadBufferPool readBufferPool,
            ScheduledExecutorService readIOExecutor,
            ResultPartitionManager partitionManager,
            int networkBufferSize,
            String dataFileBasePath,
            float minReservedDiskSpaceFraction,
            boolean isBroadcast,
            TieredStoreConfiguration storeConfiguration,
            @Nullable BufferCompressor bufferCompressor,
            SupplierWithException<BufferPool, IOException> bufferPoolFactory) {
        super(
                owningTaskName,
                partitionIndex,
                partitionId,
                partitionType,
                numSubpartitions,
                numTargetKeyGroups,
                partitionManager,
                bufferCompressor,
                bufferPoolFactory);

        this.networkBufferSize = networkBufferSize;
        this.dataFileBasePath = dataFileBasePath;
        this.minReservedDiskSpaceFraction = minReservedDiskSpaceFraction;
        this.isBroadcast = isBroadcast;
        this.storeConfiguration = storeConfiguration;
        this.numBuffersTriggerFlushRatio = storeConfiguration.getNumBuffersTriggerFlushRatio();
        this.tierExclusiveBuffers = new HashMap<>();
        this.cacheFlushManager = new CacheFlushManager();
        this.partitionFileManager =
                new PartitionFileManagerImpl(
                        Paths.get(dataFileBasePath + DATA_FILE_SUFFIX),
                        new RegionBufferIndexTrackerImpl(isBroadcast ? 1 : numSubpartitions),
                        readBufferPool,
                        readIOExecutor,
                        storeConfiguration,
                        numSubpartitions,
                        jobID,
                        partitionId,
                        storeConfiguration.getBaseDfsHomePath());
    }

    // Called by task thread.
    @Override
    protected void setupInternal() throws IOException {
        if (isReleased()) {
            throw new IOException("Result partition has been released.");
        }

        setupTierDataGates();
        tieredStoreProducer =
                new TieredStoreProducerImpl(
                        allTiers,
                        numSubpartitions,
                        networkBufferSize,
                        isBroadcast,
                        tieredStoreMemoryManager,
                        bufferCompressor);
        tieredStoreNettyService = new TieredStoreNettyServiceImpl(allTiers);
    }

    @Override
    public void setMetricGroup(TaskIOMetricGroup metrics) {
        super.setMetricGroup(metrics);
        for (StorageTier storageTier : this.allTiers) {
            storageTier.setOutputMetrics(new OutputMetrics(numBytesOut, numBuffersOut));
        }
    }

    private void setupTierDataGates() throws IOException {
        TieredStoreMode.SupportedTierCombinations supportedTierCombinations;
        try {
            supportedTierCombinations =
                    TieredStoreMode.SupportedTierCombinations.valueOf(
                            storeConfiguration.getTieredStoreTiers());
        } catch (Exception e) {
            throw new RuntimeException("Illegal tiers for Tiered Store.", e);
        }
        ResultPartitionType partitionType = getPartitionType();
        switch (supportedTierCombinations) {
            case MEMORY:
                this.allTiers = new StorageTier[1];
                addTierExclusiveBuffers(TieredStoreMode.TierType.IN_MEM);
                this.allTiers[0] = getMemoryTier();
                this.allTiers[0].setup();
                break;
            case DISK:
                this.allTiers = new StorageTier[1];
                addTierExclusiveBuffers(TieredStoreMode.TierType.IN_DISK);
                this.allTiers[0] = getDiskTier();
                this.allTiers[0].setup();
                break;
            case REMOTE:
                this.allTiers = new StorageTier[1];
                addTierExclusiveBuffers(TieredStoreMode.TierType.IN_REMOTE);
                this.allTiers[0] = getRemoteTier();
                this.allTiers[0].setup();
                break;
            case MEMORY_DISK:
                if (partitionType == HYBRID_SELECTIVE) {
                    this.allTiers = new StorageTier[2];
                    addTierExclusiveBuffers(
                            TieredStoreMode.TierType.IN_MEM, TieredStoreMode.TierType.IN_DISK);
                    this.allTiers[0] = getMemoryTier();
                    this.allTiers[1] = getDiskTier();
                    for (StorageTier tierDataGate : allTiers) {
                        tierDataGate.setup();
                    }
                } else {
                    this.allTiers = new StorageTier[1];
                    addTierExclusiveBuffers(TieredStoreMode.TierType.IN_DISK);
                    this.allTiers[0] = getDiskTier();
                    this.allTiers[0].setup();
                }
                break;
            case MEMORY_REMOTE:
                if (partitionType == HYBRID_SELECTIVE) {
                    this.allTiers = new StorageTier[2];
                    addTierExclusiveBuffers(
                            TieredStoreMode.TierType.IN_MEM, TieredStoreMode.TierType.IN_REMOTE);
                    this.allTiers[0] = getMemoryTier();
                    this.allTiers[1] = getRemoteTier();
                    for (StorageTier tierDataGate : allTiers) {
                        tierDataGate.setup();
                    }
                } else {
                    this.allTiers = new StorageTier[1];
                    addTierExclusiveBuffers(TieredStoreMode.TierType.IN_REMOTE);
                    this.allTiers[0] = getRemoteTier();
                    this.allTiers[0].setup();
                }
                break;
            case MEMORY_DISK_REMOTE:
                if (partitionType == HYBRID_SELECTIVE) {
                    this.allTiers = new StorageTier[3];
                    addTierExclusiveBuffers(
                            TieredStoreMode.TierType.IN_MEM,
                            TieredStoreMode.TierType.IN_DISK,
                            TieredStoreMode.TierType.IN_REMOTE);
                    this.allTiers[0] = getMemoryTier();
                    this.allTiers[1] = getDiskTier();
                    this.allTiers[2] = getRemoteTier();
                    for (StorageTier tierDataGate : allTiers) {
                        tierDataGate.setup();
                    }
                } else {
                    this.allTiers = new StorageTier[2];
                    addTierExclusiveBuffers(
                            TieredStoreMode.TierType.IN_DISK, TieredStoreMode.TierType.IN_REMOTE);
                    this.allTiers[0] = getDiskTier();
                    this.allTiers[1] = getRemoteTier();
                    for (StorageTier tierDataGate : allTiers) {
                        tierDataGate.setup();
                    }
                }
                break;
            case DISK_REMOTE:
                this.allTiers = new StorageTier[2];
                addTierExclusiveBuffers(
                        TieredStoreMode.TierType.IN_DISK, TieredStoreMode.TierType.IN_REMOTE);
                this.allTiers[0] = getDiskTier();
                this.allTiers[1] = getRemoteTier();
                for (StorageTier tierDataGate : allTiers) {
                    tierDataGate.setup();
                }
                break;
            default:
                throw new RuntimeException("Illegal tiers for Tiered Store.");
        }
    }

    private void addTierExclusiveBuffers(TieredStoreMode.TierType... toAddTierTypes) {
        for (TieredStoreMode.TierType toAddTierType : toAddTierTypes) {
            tierExclusiveBuffers.put(
                    toAddTierType,
                    checkNotNull(HYBRID_SHUFFLE_TIER_EXCLUSIVE_BUFFERS.get(toAddTierType)));
        }
        tieredStoreMemoryManager =
                new UpstreamTieredStoreMemoryManager(
                        bufferPool,
                        tierExclusiveBuffers,
                        numSubpartitions,
                        numBuffersTriggerFlushRatio,
                        cacheFlushManager);
    }

    private MemoryTier getMemoryTier() {
        return new MemoryTier(
                numSubpartitions,
                networkBufferSize,
                tieredStoreMemoryManager,
                isBroadcast,
                bufferCompressor);
    }

    private DiskTier getDiskTier() {
        return new DiskTier(
                numSubpartitions,
                networkBufferSize,
                getPartitionId(),
                tieredStoreMemoryManager,
                cacheFlushManager,
                dataFileBasePath,
                minReservedDiskSpaceFraction,
                isBroadcast,
                bufferCompressor,
                partitionFileManager);
    }

    private RemoteTier getRemoteTier() {
        String baseDfsPath = storeConfiguration.getBaseDfsHomePath();
        if (StringUtils.isNullOrWhitespaceOnly(baseDfsPath)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Must specify DFS home path by %s when using DFS in Tiered Store.",
                            NettyShuffleEnvironmentOptions
                                    .NETWORK_HYBRID_SHUFFLE_REMOTE_STORAGE_BASE_HOME_PATH
                                    .key()));
        }
        return new RemoteTier(
                numSubpartitions,
                networkBufferSize,
                tieredStoreMemoryManager,
                cacheFlushManager,
                isBroadcast,
                bufferCompressor,
                partitionFileManager);
    }

    @Override
    public void emitRecord(ByteBuffer record, int targetSubpartition) throws IOException {
        resultPartitionBytes.inc(targetSubpartition, record.remaining());
        emit(record, targetSubpartition, Buffer.DataType.DATA_BUFFER, false, false);
    }

    @Override
    public void broadcastRecord(ByteBuffer record) throws IOException {
        resultPartitionBytes.incAll(record.remaining());
        broadcast(record, Buffer.DataType.DATA_BUFFER, false);
    }

    @Override
    public void broadcastEvent(AbstractEvent event, boolean isPriorityEvent) throws IOException {
        Buffer buffer = EventSerializer.toBuffer(event, isPriorityEvent);
        try {
            ByteBuffer serializedEvent = buffer.getNioBufferReadable();
            broadcast(
                    serializedEvent,
                    buffer.getDataType(),
                    event.equals(EndOfPartitionEvent.INSTANCE));
        } finally {
            buffer.recycleBuffer();
        }
    }

    private void broadcast(ByteBuffer record, Buffer.DataType dataType, boolean isEndOfPartition)
            throws IOException {
        checkInProduceState();
        emit(record, 0, dataType, true, isEndOfPartition);
    }

    private void emit(
            ByteBuffer record,
            int targetSubpartition,
            Buffer.DataType dataType,
            boolean isBroadcast,
            boolean isEndOfPartition)
            throws IOException {
        checkNotNull(tieredStoreProducer)
                .emit(record, targetSubpartition, dataType, isBroadcast, isEndOfPartition);
    }

    @Override
    public ResultSubpartitionView createSubpartitionView(
            int subpartitionId, BufferAvailabilityListener availabilityListener)
            throws IOException {
        checkState(!isReleased(), "ResultPartition already released.");
        return tieredStoreNettyService.register(subpartitionId, availabilityListener);
    }

    @Override
    public void finish() throws IOException {
        broadcastEvent(EndOfPartitionEvent.INSTANCE, false);
        checkState(!isReleased(), "Result partition is already released.");
        super.finish();
    }

    @Override
    public void close() {
        // close is called when task is finished or failed.
        super.close();
        // first close the writer
        if (tieredStoreProducer != null) {
            tieredStoreProducer.close();
        }
        if (tieredStoreMemoryManager != null) {
            tieredStoreMemoryManager.close();
        }
        if (cacheFlushManager != null) {
            cacheFlushManager.close();
        }
    }

    @Override
    protected void releaseInternal() {
        // release is called when release by scheduler, later than close.
        // mainly work :
        // 1. release read scheduler.
        // 2. delete shuffle file.
        // 3. release all data in memory.

        // first release the writer
        tieredStoreProducer.release();
        tieredStoreMemoryManager.release();
    }

    @Override
    public void alignedBarrierTimeout(long checkpointId) throws IOException {
        // Nothing to do.
    }

    @Override
    public void abortCheckpoint(long checkpointId, CheckpointException cause) {
        // Nothing to do.
    }

    @Override
    public void flushAll() {
        // Nothing to do.
    }

    @Override
    public void flush(int subpartitionIndex) {
        // Nothing to do.
    }

    @Override
    public void setChannelStateWriter(ChannelStateWriter channelStateWriter) {
        // Nothing to do.
    }

    @Override
    public CompletableFuture<Void> getAllDataProcessedFuture() {
        // Nothing to do.
        return FutureUtils.completedVoidFuture();
    }

    @Override
    public void onSubpartitionAllDataProcessed(int subpartition) {
        // Nothing to do.
    }

    @Override
    public int getNumberOfQueuedBuffers() {
        // Nothing to do.
        return 0;
    }

    @Override
    public long getSizeOfQueuedBuffersUnsafe() {
        // Nothing to do.
        return 0;
    }

    @Override
    public int getNumberOfQueuedBuffers(int targetSubpartition) {
        // Nothing to do.
        return 0;
    }

    @Override
    public void notifyEndOfData(StopMode mode) throws IOException {
        if (!hasNotifiedEndOfUserRecords) {
            broadcastEvent(new EndOfData(mode), false);
            hasNotifiedEndOfUserRecords = true;
        }
    }

    @VisibleForTesting
    public List<Path> getBaseSubpartitionPath(int subpartitionId) {
        List<Path> paths = new ArrayList<>();
        for (StorageTier tierDataGate : allTiers) {
            paths.add(tierDataGate.getBaseSubpartitionPath(subpartitionId));
        }
        return paths;
    }
}
