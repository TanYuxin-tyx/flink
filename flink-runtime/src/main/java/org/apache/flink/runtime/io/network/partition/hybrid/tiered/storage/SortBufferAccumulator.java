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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.function.BiConsumer;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The hash implementation of the {@link BufferAccumulator}. The {@link BufferAccumulator} receives
 * the records from {@link TieredStorageProducerClient} and the records will accumulate and
 * transform to finished buffers. The accumulated buffers will be transferred to the corresponding
 * tier dynamically.
 *
 * <p>TODO, add doc
 *
 * <p>Note that this class need not be thread-safe, because it should only be accessed from the main
 * thread.
 */
public class SortBufferAccumulator implements BufferAccumulator {

    private final int numBuffersOfSortAccumulatorThreshold;

    private final int numSubpartitions;

    private final int bufferSize;

    private final TieredStorageMemoryManager memoryManager;

    private final LinkedList<BufferBuilder> availableBuffers;

    private SortBufferContainer broadcastSortBufferContainer;

    private SortBufferContainer unicastSortBufferContainer;

    /**
     * The {@link HashBufferAccumulator}'s accumulated buffer flusher is not prepared during
     * construction, requiring the field to be initialized during setup. Therefore, it is necessary
     * to verify whether this field is null before using it.
     */
    @Nullable
    private BiConsumer<TieredStorageSubpartitionId, List<Buffer>> accumulatedBufferFlusher;

    public SortBufferAccumulator(
            TieredStoragePartitionId partitionId,
            int numSubpartitions,
            int numBuffersOfSortAccumulatorThreshold,
            int bufferSize,
            TieredStorageMemoryManager memoryManager,
            TieredStorageResourceRegistry resourceRegistry) {
        this.numBuffersOfSortAccumulatorThreshold = numBuffersOfSortAccumulatorThreshold;
        this.numSubpartitions = numSubpartitions;
        this.bufferSize = bufferSize;
        this.memoryManager = memoryManager;
        this.availableBuffers = new LinkedList<>();
        resourceRegistry.registerResource(partitionId, this::releaseResources);
    }

    @Override
    public void setup(BiConsumer<TieredStorageSubpartitionId, List<Buffer>> bufferFlusher) {
        this.accumulatedBufferFlusher = bufferFlusher;
    }

    @Override
    public void receive(
            ByteBuffer record,
            TieredStorageSubpartitionId subpartitionId,
            Buffer.DataType dataType,
            boolean isBroadcast,
            boolean isEndOfPartition)
            throws IOException {
        if (isEndOfPartition) {
            flushUnicastSortBufferContainer();
            flushBroadcastSortBufferContainer();
        }
        int subpartitionIndex = isBroadcast ? 0 : subpartitionId.getSubpartitionId();
        SortBufferContainer sortBufferContainer =
                isBroadcast
                        ? createBroadcastSortBufferContainer()
                        : createUnicastSortBufferContainer();
        if (!sortBufferContainer.append(record, subpartitionIndex, dataType)) {
            return;
        }

        if (!sortBufferContainer.hasRemaining()) {
            sortBufferContainer.release();
            writeLargeRecord(record, subpartitionIndex, dataType);
            return;
        }

        flushSortBufferContainer(sortBufferContainer);
        sortBufferContainer.release();
        if (record.hasRemaining()) {
            receive(record, subpartitionId, dataType, isBroadcast, isEndOfPartition);
        }
    }

    @Override
    public void close() {}

    private void requestAvailableBuffers() {
        while (availableBuffers.size() < numBuffersOfSortAccumulatorThreshold) {
            availableBuffers.add(memoryManager.requestBufferBlocking(this));
        }
    }

    private void writeLargeRecord(
            ByteBuffer record, int targetSubpartition, Buffer.DataType dataType) {
        checkState(dataType != Buffer.DataType.EVENT_BUFFER);
        while (record.hasRemaining()) {
            int toCopy = Math.min(record.remaining(), bufferSize);
            BufferBuilder builder = getAvailableBuffer();
            Buffer buffer = builder.createBufferConsumerFromBeginning().build();
            MemorySegment writeBuffer = buffer.getMemorySegment();
            writeBuffer.put(0, record, toCopy);

            NetworkBuffer networkBuffer =
                    new NetworkBuffer(
                            writeBuffer, buffer.getRecycler(), dataType, writeBuffer.size());
            flushAccumulatedBuffer(
                    new TieredStorageSubpartitionId(targetSubpartition),
                    Collections.singletonList(networkBuffer));
        }

        recycleAvailableBuffers();
    }

    private SortBufferContainer createBroadcastSortBufferContainer() {
        flushUnicastSortBufferContainer();

        if (broadcastSortBufferContainer != null
                && !broadcastSortBufferContainer.isFinished()
                && !broadcastSortBufferContainer.isReleased()) {
            return broadcastSortBufferContainer;
        }

        broadcastSortBufferContainer = createNewBufferContainer();
        return broadcastSortBufferContainer;
    }

    private SortBufferContainer createUnicastSortBufferContainer() {
        flushBroadcastSortBufferContainer();

        if (unicastSortBufferContainer != null
                && !unicastSortBufferContainer.isFinished()
                && !unicastSortBufferContainer.isReleased()) {
            return unicastSortBufferContainer;
        }

        unicastSortBufferContainer = createNewBufferContainer();
        return unicastSortBufferContainer;
    }

    private SortBufferContainer createNewBufferContainer() {
        requestAvailableBuffers();
        return new SortBufferContainer(
                availableBuffers,
                numSubpartitions,
                bufferSize,
                numBuffersOfSortAccumulatorThreshold,
                null);
    }

    private void flushBroadcastSortBufferContainer() {
        if (broadcastSortBufferContainer != null) {
            flushSortBufferContainer(broadcastSortBufferContainer);
            broadcastSortBufferContainer.release();
            broadcastSortBufferContainer = null;
        }
    }

    private void flushUnicastSortBufferContainer() {
        if (unicastSortBufferContainer != null) {
            flushSortBufferContainer(unicastSortBufferContainer);
            unicastSortBufferContainer.release();
            unicastSortBufferContainer = null;
        }
    }

    private void flushSortBufferContainer(SortBufferContainer sortBufferContainer) {
        if (sortBufferContainer == null
                || sortBufferContainer.isReleased()
                || !sortBufferContainer.hasRemaining()) {
            return;
        }
        sortBufferContainer.finish();

        do {
            BufferBuilder builder = getAvailableBuffer();
            Buffer buffer = builder.createBufferConsumerFromBeginning().build();
            MemorySegmentAndChannel memorySegmentAndChannel =
                    sortBufferContainer.getNextBuffer(buffer.getMemorySegment());
            if (memorySegmentAndChannel == null) {
                buffer.recycleBuffer();
                break;
            }
            NetworkBuffer networkBuffer =
                    new NetworkBuffer(
                            memorySegmentAndChannel.getBuffer(),
                            buffer.getRecycler(),
                            memorySegmentAndChannel.getDataType(),
                            memorySegmentAndChannel.getDataSize());
            flushAccumulatedBuffer(
                    new TieredStorageSubpartitionId(memorySegmentAndChannel.getChannelIndex()),
                    Collections.singletonList(networkBuffer));
        } while (true);

        recycleAvailableBuffers();
        sortBufferContainer.release();
    }

    private BufferBuilder getAvailableBuffer() {
        BufferBuilder builder = availableBuffers.poll();
        if (builder == null) {
            builder = memoryManager.requestBufferBlocking(this);
        }
        return builder;
    }

    private void flushAccumulatedBuffer(
            TieredStorageSubpartitionId subpartitionId, List<Buffer> accumulatedBuffers) {
        checkNotNull(accumulatedBufferFlusher).accept(subpartitionId, accumulatedBuffers);
    }

    private void recycleAvailableBuffers() {
        availableBuffers.forEach(SortBufferAccumulator::recycleBufferBuilder);
    }

    private static void recycleBufferBuilder(BufferBuilder bufferBuilder) {
        bufferBuilder.createBufferConsumerFromBeginning().build().recycleBuffer();
    }

    private void releaseResources() {
        if (broadcastSortBufferContainer != null && !broadcastSortBufferContainer.isReleased()) {
            broadcastSortBufferContainer.release();
        }
        if (unicastSortBufferContainer != null && !unicastSortBufferContainer.isReleased()) {
            unicastSortBufferContainer.release();
        }
    }
}
