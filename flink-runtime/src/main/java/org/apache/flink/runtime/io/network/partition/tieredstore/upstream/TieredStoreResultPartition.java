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
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.BufferPoolHelper;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.BufferPoolHelperImpl;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.StorageTier;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreProducer;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.DiskTier;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.OutputMetrics;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.memory.MemoryTier;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.remote.RemoteTier;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.writer.TieredStoreProducerImpl;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.runtime.io.network.partition.tieredstore.upstream.TieredStoreMode.SpillingType.FULL;
import static org.apache.flink.runtime.io.network.partition.tieredstore.upstream.TieredStoreMode.SpillingType.NO_FLUSH;
import static org.apache.flink.runtime.io.network.partition.tieredstore.upstream.TieredStoreMode.Tiers.DFS;
import static org.apache.flink.runtime.io.network.partition.tieredstore.upstream.TieredStoreMode.Tiers.LOCAL;
import static org.apache.flink.runtime.io.network.partition.tieredstore.upstream.TieredStoreMode.Tiers.LOCAL_DFS;
import static org.apache.flink.runtime.io.network.partition.tieredstore.upstream.TieredStoreMode.Tiers.MEMORY;
import static org.apache.flink.runtime.io.network.partition.tieredstore.upstream.TieredStoreMode.Tiers.MEMORY_DFS;
import static org.apache.flink.runtime.io.network.partition.tieredstore.upstream.TieredStoreMode.Tiers.MEMORY_LOCAL;
import static org.apache.flink.runtime.io.network.partition.tieredstore.upstream.TieredStoreMode.Tiers.MEMORY_LOCAL_DFS;
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

    private final boolean isBroadcast;

    private BufferPoolHelper bufferPoolHelper;

    private StorageTier[] tierDataGates;

    private TieredStoreProducer tieredStoreProducer;

    private boolean hasNotifiedEndOfUserRecords;

    private final ResultSubpartition[] subpartitions;

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
            boolean isBroadcast,
            TieredStoreConfiguration storeConfiguration,
            @Nullable BufferCompressor bufferCompressor,
            SupplierWithException<BufferPool, IOException> bufferPoolFactory,
            ResultSubpartition[] subpartitions) {
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
        this.isBroadcast = isBroadcast;
        this.storeConfiguration = storeConfiguration;
        this.subpartitions = subpartitions;
    }

    // Called by task thread.
    @Override
    protected void setupInternal() throws IOException {
        if (isReleased()) {
            throw new IOException("Result partition has been released.");
        }
        bufferPoolHelper =
                new BufferPoolHelperImpl(
                        bufferPool,
                        storeConfiguration.getTieredStoreBufferInMemoryRatio(),
                        storeConfiguration.getTieredStoreFlushBufferRatio(),
                        storeConfiguration.getTieredStoreTriggerFlushRatio(),
                        numSubpartitions);
        setupTierDataGates();
        tieredStoreProducer =
                new TieredStoreProducerImpl(tierDataGates, numSubpartitions, isBroadcast);
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
        TieredStoreMode.SpillingType spillingType;
        try {
            tiers = TieredStoreMode.Tiers.valueOf(storeConfiguration.getTieredStoreTiers());
            spillingType =
                    TieredStoreMode.SpillingType.valueOf(
                            storeConfiguration.getTieredStoreSpillingType());
        } catch (Exception e) {
            throw new RuntimeException("Illegal tiers or spilling types for Tiered Store.", e);
        }

        switch (tiers) {
            case MEMORY:
                checkState(
                        spillingType == NO_FLUSH,
                        "spilling type must be %s if the tiers is %s",
                        NO_FLUSH,
                        MEMORY);
                this.tierDataGates = new StorageTier[1];
                this.tierDataGates[0] = getLocalMemoryDataManager();
                this.tierDataGates[0].setup();
                break;
            case LOCAL:
                checkState(
                        spillingType != NO_FLUSH,
                        "spilling type must not be %s if the tiers is %s",
                        NO_FLUSH,
                        LOCAL);
                this.tierDataGates = new StorageTier[1];
                this.tierDataGates[0] = getLocalFileDataManager();
                this.tierDataGates[0].setup();
                break;
            case DFS:
                checkState(
                        spillingType == FULL,
                        "spilling type must be %s if the tiers is %s",
                        FULL,
                        DFS);
                this.tierDataGates = new StorageTier[1];
                this.tierDataGates[0] = getDfsDataManager();
                this.tierDataGates[0].setup();
                break;
            case MEMORY_LOCAL:
                checkState(
                        spillingType != NO_FLUSH,
                        "spilling type must not be %s if the tiers is %s",
                        NO_FLUSH,
                        MEMORY_LOCAL);
                this.tierDataGates = new StorageTier[2];
                this.tierDataGates[0] = getLocalMemoryDataManager();
                this.tierDataGates[1] = getLocalFileDataManager();
                for (StorageTier tierDataGate : tierDataGates) {
                    tierDataGate.setup();
                }
                break;
            case MEMORY_DFS:
                checkState(
                        spillingType != NO_FLUSH,
                        "spilling type must not be %s if the tiers is %s",
                        NO_FLUSH,
                        MEMORY_DFS);
                this.tierDataGates = new StorageTier[2];
                this.tierDataGates[0] = getLocalMemoryDataManager();
                this.tierDataGates[1] = getDfsDataManager();
                for (StorageTier tierDataGate : tierDataGates) {
                    tierDataGate.setup();
                }
                break;
            case MEMORY_LOCAL_DFS:
                checkState(
                        spillingType != NO_FLUSH,
                        "spilling type must not be %s if the tiers is %s",
                        NO_FLUSH,
                        MEMORY_LOCAL_DFS);
                this.tierDataGates = new StorageTier[3];
                this.tierDataGates[0] = getLocalMemoryDataManager();
                this.tierDataGates[1] = getLocalFileDataManager();
                this.tierDataGates[2] = getDfsDataManager();
                for (StorageTier tierDataGate : tierDataGates) {
                    tierDataGate.setup();
                }
                break;
            case LOCAL_DFS:
                checkState(
                        spillingType == FULL,
                        "spilling type must be %s if the tiers is %s",
                        FULL,
                        LOCAL_DFS);
                this.tierDataGates = new StorageTier[2];
                this.tierDataGates[0] = getLocalFileDataManager();
                this.tierDataGates[1] = getDfsDataManager();
                for (StorageTier tierDataGate : tierDataGates) {
                    tierDataGate.setup();
                }
                break;
            default:
                throw new RuntimeException("Illegal tiers for Tiered Store.");
        }
    }

    private MemoryTier getLocalMemoryDataManager() {
        return new MemoryTier(
                numSubpartitions,
                networkBufferSize,
                bufferPoolHelper,
                isBroadcast,
                bufferCompressor,
                storeConfiguration);
    }

    private DiskTier getLocalFileDataManager() {
        return new DiskTier(
                numSubpartitions,
                networkBufferSize,
                getPartitionId(),
                bufferPoolHelper,
                dataFileBasePath,
                isBroadcast,
                bufferCompressor,
                readBufferPool,
                readIOExecutor,
                storeConfiguration);
    }

    private RemoteTier getDfsDataManager() throws IOException {
        String baseDfsPath = storeConfiguration.getBaseDfsHomePath();
        if (StringUtils.isNullOrWhitespaceOnly(baseDfsPath)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Must specify DFS home path by %s when using DFS in Tiered Store.",
                            NettyShuffleEnvironmentOptions.SHUFFLE_BASE_DFS_HOME_PATH.key()));
        }
        return new RemoteTier(
                jobID,
                numSubpartitions,
                networkBufferSize,
                getPartitionId(),
                bufferPoolHelper,
                isBroadcast,
                baseDfsPath,
                bufferCompressor);
    }

    @Override
    public void emitRecord(ByteBuffer record, int targetSubpartition) throws IOException {
        emit(record, targetSubpartition, Buffer.DataType.DATA_BUFFER, false, false);
    }

    @Override
    public void broadcastRecord(ByteBuffer record) throws IOException {
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

    private int[] createdSubpartitions = new int[10];

    @Override
    public ResultSubpartitionView createSubpartitionView(
            int subpartitionId, BufferAvailabilityListener availabilityListener)
            throws IOException {
        createdSubpartitions[subpartitionId] = 1;
        checkState(!isReleased(), "ResultPartition already released.");
        return new TieredStoreSubpartitionViewDelegate(
                subpartitionId, availabilityListener, tierDataGates, getOwningTaskName());
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
        tieredStoreProducer.close();
        bufferPoolHelper.close();
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
