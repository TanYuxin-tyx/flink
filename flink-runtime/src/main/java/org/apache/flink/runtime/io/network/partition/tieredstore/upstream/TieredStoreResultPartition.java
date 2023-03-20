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
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.StorageTier;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreMemoryManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreProducer;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.UpstreamTieredStoreMemoryManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.file.PartitionFileManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.file.PartitionFileManagerImpl;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.DiskTier;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.OutputMetrics;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.RegionBufferIndexTrackerImpl;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.memory.MemoryTier;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.remote.RemoteTier;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.util.StringUtils;
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
import static org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.DiskTier.DATA_FILE_SUFFIX;
import static org.apache.flink.runtime.shuffle.NettyShuffleUtils.HYBRID_SHUFFLE_TIER_EXCLUSIVE_BUFFERS;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** ResultPartition for TieredStore. */
public class TieredStoreResultPartition extends ResultPartition implements ChannelStateHolder {

    private final JobID jobID;

    private final BatchShuffleReadBufferPool readBufferPool;

    private final ScheduledExecutorService readIOExecutor;

    private final int networkBufferSize;

    private final TieredStoreConfiguration storeConfiguration;

    private final String dataFileBasePath;

    private final float minReservedDiskSpaceFraction;

    private final boolean isBroadcast;

    private final CacheFlushManager cacheFlushManager;

    public final Map<TieredStoreMode.TieredType, Integer> tierExclusiveBuffers;

    private StorageTier[] tierDataGates;

    private TieredStoreProducer tieredStoreProducer;

    private boolean hasNotifiedEndOfUserRecords;

    private TieredStoreMemoryManager tieredStoreMemoryManager;

    private PartitionFileManager partitionFileManager;

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

        this.jobID = jobID;
        this.readBufferPool = readBufferPool;
        this.readIOExecutor = readIOExecutor;
        this.networkBufferSize = networkBufferSize;
        this.dataFileBasePath = dataFileBasePath;
        this.minReservedDiskSpaceFraction = minReservedDiskSpaceFraction;
        this.isBroadcast = isBroadcast;
        this.storeConfiguration = storeConfiguration;
        this.tierExclusiveBuffers = new HashMap<>();
        this.cacheFlushManager = new CacheFlushManager();
        this.partitionFileManager =
                new PartitionFileManagerImpl(
                        Paths.get(dataFileBasePath + DATA_FILE_SUFFIX),
                        new RegionBufferIndexTrackerImpl(isBroadcast ? 1 : numSubpartitions),
                        readBufferPool,
                        readIOExecutor,
                        storeConfiguration);
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
                        tierDataGates,
                        numSubpartitions,
                        networkBufferSize,
                        bufferCompressor,
                        isBroadcast,
                        tieredStoreMemoryManager);
    }

    @Override
    public void setMetricGroup(TaskIOMetricGroup metrics) {
        super.setMetricGroup(metrics);
        for (StorageTier storageTier : this.tierDataGates) {
            storageTier.setOutputMetrics(new OutputMetrics(numBytesOut, numBuffersOut));
            storageTier.setTimerGauge(metrics.getHardBackPressuredTimePerSecond());
        }
    }

    private void setupTierDataGates() throws IOException {
        TieredStoreMode.Tiers tiers;
        try {
            tiers = TieredStoreMode.Tiers.valueOf(storeConfiguration.getTieredStoreTiers());
        } catch (Exception e) {
            throw new RuntimeException("Illegal tiers for Tiered Store.", e);
        }
        ResultPartitionType partitionType = getPartitionType();
        switch (tiers) {
            case MEMORY:
                this.tierDataGates = new StorageTier[1];
                addTierExclusiveBuffers(TieredStoreMode.TieredType.IN_MEM);
                this.tierDataGates[0] = getMemoryTier();
                this.tierDataGates[0].setup();
                break;
            case LOCAL:
                this.tierDataGates = new StorageTier[1];
                addTierExclusiveBuffers(TieredStoreMode.TieredType.IN_LOCAL);
                this.tierDataGates[0] = getDiskTier();
                this.tierDataGates[0].setup();
                break;
            case DFS:
                this.tierDataGates = new StorageTier[1];
                addTierExclusiveBuffers(TieredStoreMode.TieredType.IN_DFS);
                this.tierDataGates[0] = getRemoteTier();
                this.tierDataGates[0].setup();
                break;
            case MEMORY_LOCAL:
                if (partitionType == HYBRID_SELECTIVE) {
                    this.tierDataGates = new StorageTier[2];
                    addTierExclusiveBuffers(
                            TieredStoreMode.TieredType.IN_MEM, TieredStoreMode.TieredType.IN_LOCAL);
                    this.tierDataGates[0] = getMemoryTier();
                    this.tierDataGates[1] = getDiskTier();
                    for (StorageTier tierDataGate : tierDataGates) {
                        tierDataGate.setup();
                    }
                    break;
                } else {
                    this.tierDataGates = new StorageTier[1];
                    addTierExclusiveBuffers(TieredStoreMode.TieredType.IN_LOCAL);
                    this.tierDataGates[0] = getDiskTier();
                    this.tierDataGates[0].setup();
                    break;
                }
            case MEMORY_DFS:
                if (partitionType == HYBRID_SELECTIVE) {
                    this.tierDataGates = new StorageTier[2];
                    addTierExclusiveBuffers(
                            TieredStoreMode.TieredType.IN_MEM, TieredStoreMode.TieredType.IN_DFS);
                    this.tierDataGates[0] = getMemoryTier();
                    this.tierDataGates[1] = getRemoteTier();
                    for (StorageTier tierDataGate : tierDataGates) {
                        tierDataGate.setup();
                    }
                    break;
                } else {
                    this.tierDataGates = new StorageTier[1];
                    addTierExclusiveBuffers(TieredStoreMode.TieredType.IN_DFS);
                    this.tierDataGates[0] = getRemoteTier();
                    this.tierDataGates[0].setup();
                    break;
                }
            case MEMORY_LOCAL_DFS:
                if (partitionType == HYBRID_SELECTIVE) {
                    this.tierDataGates = new StorageTier[3];
                    addTierExclusiveBuffers(
                            TieredStoreMode.TieredType.IN_MEM,
                            TieredStoreMode.TieredType.IN_LOCAL,
                            TieredStoreMode.TieredType.IN_DFS);
                    this.tierDataGates[0] = getMemoryTier();
                    this.tierDataGates[1] = getDiskTier();
                    this.tierDataGates[2] = getRemoteTier();
                    for (StorageTier tierDataGate : tierDataGates) {
                        tierDataGate.setup();
                    }
                    break;
                } else {
                    this.tierDataGates = new StorageTier[2];
                    addTierExclusiveBuffers(
                            TieredStoreMode.TieredType.IN_LOCAL, TieredStoreMode.TieredType.IN_DFS);
                    this.tierDataGates[0] = getDiskTier();
                    this.tierDataGates[1] = getRemoteTier();
                    for (StorageTier tierDataGate : tierDataGates) {
                        tierDataGate.setup();
                    }
                    break;
                }
            case LOCAL_DFS:
                this.tierDataGates = new StorageTier[2];
                addTierExclusiveBuffers(
                        TieredStoreMode.TieredType.IN_LOCAL, TieredStoreMode.TieredType.IN_DFS);
                this.tierDataGates[0] = getDiskTier();
                this.tierDataGates[1] = getRemoteTier();
                for (StorageTier tierDataGate : tierDataGates) {
                    tierDataGate.setup();
                }
                break;
            default:
                throw new RuntimeException("Illegal tiers for Tiered Store.");
        }
    }

    private void addTierExclusiveBuffers(TieredStoreMode.TieredType... tieredTypes) {
        for (TieredStoreMode.TieredType tieredType : tieredTypes) {
            tierExclusiveBuffers.put(
                    tieredType,
                    checkNotNull(HYBRID_SHUFFLE_TIER_EXCLUSIVE_BUFFERS.get(tieredType)));
        }
        tieredStoreMemoryManager =
                new UpstreamTieredStoreMemoryManager(
                        bufferPool, tierExclusiveBuffers, numSubpartitions, cacheFlushManager);
    }

    private MemoryTier getMemoryTier() {
        addTierExclusiveBuffers(TieredStoreMode.TieredType.IN_MEM);
        return new MemoryTier(
                numSubpartitions,
                networkBufferSize,
                tieredStoreMemoryManager,
                isBroadcast,
                bufferCompressor);
    }

    private DiskTier getDiskTier() {
        addTierExclusiveBuffers(TieredStoreMode.TieredType.IN_LOCAL);
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
                readBufferPool,
                readIOExecutor,
                storeConfiguration,
                partitionFileManager);
    }

    private RemoteTier getRemoteTier() throws IOException {
        addTierExclusiveBuffers(TieredStoreMode.TieredType.IN_DFS);
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
                jobID,
                numSubpartitions,
                networkBufferSize,
                getPartitionId(),
                tieredStoreMemoryManager,
                cacheFlushManager,
                isBroadcast,
                baseDfsPath,
                bufferCompressor);
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
            if (event.equals(EndOfPartitionEvent.INSTANCE)) {
                broadcast(serializedEvent, buffer.getDataType(), true);
            } else {
                broadcast(serializedEvent, buffer.getDataType(), false);
            }
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
        return new TieredStoreSubpartitionViewDelegate(
                subpartitionId, availabilityListener, tierDataGates);
    }

    @Override
    public void alignedBarrierTimeout(long checkpointId) throws IOException {
        tieredStoreProducer.alignedBarrierTimeout(checkpointId);
    }

    @Override
    public void abortCheckpoint(long checkpointId, CheckpointException cause) {
        tieredStoreProducer.abortCheckpoint(checkpointId, cause);
    }

    @Override
    public void flushAll() {
        tieredStoreProducer.flushAll();
    }

    @Override
    public void flush(int subpartitionIndex) {
        tieredStoreProducer.flush(subpartitionIndex);
    }

    @Override
    public void setChannelStateWriter(ChannelStateWriter channelStateWriter) {
        tieredStoreProducer.setChannelStateWriter(channelStateWriter);
    }

    @Override
    public void onConsumedSubpartition(int subpartitionIndex) {
        tieredStoreProducer.onConsumedSubpartition(subpartitionIndex);
    }

    @Override
    public CompletableFuture<Void> getAllDataProcessedFuture() {
        return tieredStoreProducer.getAllDataProcessedFuture();
    }

    @Override
    public void onSubpartitionAllDataProcessed(int subpartition) {
        tieredStoreProducer.onSubpartitionAllDataProcessed(subpartition);
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
    }

    @Override
    public int getNumberOfQueuedBuffers() {
        return tieredStoreProducer.getNumberOfQueuedBuffers();
    }

    @Override
    public long getSizeOfQueuedBuffersUnsafe() {
        return tieredStoreProducer.getSizeOfQueuedBuffersUnsafe();
    }

    @Override
    public int getNumberOfQueuedBuffers(int targetSubpartition) {
        return tieredStoreProducer.getNumberOfQueuedBuffers(targetSubpartition);
    }

    @Override
    public void notifyEndOfData(StopMode mode) throws IOException {
        if (!hasNotifiedEndOfUserRecords) {
            broadcastEvent(new EndOfData(mode), false);
            hasNotifiedEndOfUserRecords = true;
        }
    }

    @VisibleForTesting
    public void setNumBytesInASegment(int numBytesInASegment) {
        this.tieredStoreProducer.setNumBytesInASegment(numBytesInASegment);
    }

    @VisibleForTesting
    public List<Path> getBaseSubpartitionPath(int subpartitionId) {
        List<Path> paths = new ArrayList<>();
        for (StorageTier tierDataGate : tierDataGates) {
            paths.add(tierDataGate.getBaseSubpartitionPath(subpartitionId));
        }
        return paths;
    }
}
