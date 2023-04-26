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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.shuffle;

import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.OutputMetrics;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageIdMappingUtils;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.CacheFlushManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.ResourceRegistry;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageProducerClient;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.service.TieredStoreNettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.service.TieredStoreNettyServiceImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierProducerAgent;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link TieredResultPartition} appends records and events to the tiered store, which supports the
 * upstream dynamically switches storage tier for writing shuffle data, and the downstream will read
 * data from the relevant storage tier.
 */
public class TieredResultPartition extends ResultPartition {

    private final CacheFlushManager cacheFlushManager;

    private final List<TierProducerAgent> tierProducerAgents;

    private final TieredStorageProducerClient tieredStorageProducerClient;

    private boolean hasNotifiedEndOfUserRecords;

    private final TieredStorageMemoryManager storageMemoryManager;

    private final ResourceRegistry resourceRegistry;

    private final TieredStoragePartitionId storagePartitionId;

    private TieredStoreNettyService tieredStoreNettyService;

    public TieredResultPartition(
            String owningTaskName,
            int partitionIndex,
            ResultPartitionID partitionId,
            ResultPartitionType partitionType,
            int numSubpartitions,
            int numTargetKeyGroups,
            ResultPartitionManager partitionManager,
            List<TierProducerAgent> tierProducerAgents,
            TieredStorageMemoryManager storageMemoryManager,
            CacheFlushManager cacheFlushManager,
            @Nullable BufferCompressor bufferCompressor,
            TieredStorageProducerClient tieredStorageProducerClient,
            SupplierWithException<BufferPool, IOException> bufferPoolFactory,
            ResourceRegistry resourceRegistry) {
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

        this.tierProducerAgents = tierProducerAgents;
        this.storageMemoryManager = storageMemoryManager;
        this.cacheFlushManager = cacheFlushManager;
        this.tieredStorageProducerClient = tieredStorageProducerClient;
        this.resourceRegistry = resourceRegistry;
        this.storagePartitionId = TieredStorageIdMappingUtils.convertId(partitionId);
    }

    // Called by task thread.
    @Override
    protected void setupInternal() throws IOException {
        if (isReleased()) {
            throw new IOException("Result partition has been released.");
        }
        storageMemoryManager.setup(bufferPool);
        cacheFlushManager.setup(storageMemoryManager);
        tieredStoreNettyService = new TieredStoreNettyServiceImpl(tierProducerAgents);
        resourceRegistry.registerResource(storagePartitionId, tieredStorageProducerClient::release);
        resourceRegistry.registerResource(storagePartitionId, storageMemoryManager::release);
    }

    @Override
    public void setMetricGroup(TaskIOMetricGroup metrics) {
        super.setMetricGroup(metrics);
        tieredStorageProducerClient.setMetricGroup(new OutputMetrics(numBytesOut, numBuffersOut));
    }

    @Override
    public void emitRecord(ByteBuffer record, int consumerId) throws IOException {
        resultPartitionBytes.inc(consumerId, record.remaining());
        emit(record, consumerId, Buffer.DataType.DATA_BUFFER, false);
    }

    @Override
    public void broadcastRecord(ByteBuffer record) throws IOException {
        resultPartitionBytes.incAll(record.remaining());
        broadcast(record, Buffer.DataType.DATA_BUFFER);
    }

    @Override
    public void broadcastEvent(AbstractEvent event, boolean isPriorityEvent) throws IOException {
        Buffer buffer = EventSerializer.toBuffer(event, isPriorityEvent);
        try {
            ByteBuffer serializedEvent = buffer.getNioBufferReadable();
            broadcast(serializedEvent, buffer.getDataType());
        } finally {
            buffer.recycleBuffer();
        }
    }

    private void broadcast(ByteBuffer record, Buffer.DataType dataType) throws IOException {
        checkInProduceState();
        emit(record, 0, dataType, true);
    }

    private void emit(
            ByteBuffer record, int consumerId, Buffer.DataType dataType, boolean isBroadcast)
            throws IOException {
        checkNotNull(tieredStorageProducerClient).emit(record, consumerId, dataType, isBroadcast);
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
        if (tieredStorageProducerClient != null) {
            tieredStorageProducerClient.close();
        }

        if (cacheFlushManager != null) {
            cacheFlushManager.close();
        }
    }

    @Override
    public void notifyEndOfData(StopMode mode) throws IOException {
        if (!hasNotifiedEndOfUserRecords) {
            broadcastEvent(new EndOfData(mode), false);
            hasNotifiedEndOfUserRecords = true;
        }
    }

    @Override
    protected void releaseInternal() {
        resourceRegistry.clearResourceFor(storagePartitionId);
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
    public int getNumberOfQueuedBuffers(int consumerId) {
        // Nothing to do.
        return 0;
    }
}