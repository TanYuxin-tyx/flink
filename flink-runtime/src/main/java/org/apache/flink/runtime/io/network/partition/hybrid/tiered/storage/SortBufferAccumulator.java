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
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;

import org.apache.commons.lang3.tuple.Pair;

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
 * The sort-based implementation of the {@link BufferAccumulator}. The {@link BufferAccumulator}
 * receives the records from {@link TieredStorageProducerClient} and the records will accumulate and
 * transform to finished buffers. The accumulated buffers will be transferred to the corresponding
 * tier dynamically.
 *
 * <p>The {@link BufferAccumulator} can help use less buffers to accumulate data, which decouples
 * the buffer usage with the number of parallelism. The number of buffers used by the {@link
 * SortBufferAccumulator} will be numBuffers at most. Once the {@link SortBuffer} is full, or
 * receiving a different type of buffer, or receiving the end-of-partition event, the buffer in the
 * sort buffer will be flushed to the tiers.
 *
 * <p>Note that this class need not be thread-safe, because it should only be accessed from the main
 * thread.
 */
public class SortBufferAccumulator implements BufferAccumulator {

    /** The number of the subpartitions. */
    private final int numSubpartitions;

    /** The total number of the buffers used by the {@link SortBufferAccumulator}. */
    private final int numBuffers;

    /** The byte size of one single buffer. */
    private final int bufferSizeBytes;

    /** The empty buffers without storing data. */
    private final LinkedList<MemorySegment> freeSegments = new LinkedList<>();

    /** The memory manager of the tiered storage. */
    private final TieredStorageMemoryManager storeMemoryManager;

    /** The number of buffers for sorting used in the {@link SortBuffer}. */
    private int numBuffersForSort;

    /**
     * The {@link SortBuffer} is utilized to accumulate the incoming records. Whenever there is a
     * transition from broadcast to non-broadcast (or vice versa), the buffer is flushed to ensure
     * data integrity. Note that this can be null before using it to store records, and this {@link
     * SortBuffer} will be released once flushed.
     */
    @Nullable private SortBuffer currentDataBuffer;

    /**
     * The buffer recycler. Note that this can be null before requesting buffers from the memory
     * manager.
     */
    @Nullable private BufferRecycler bufferRecycler;

    /**
     * The {@link SortBufferAccumulator}'s accumulated buffer flusher is not prepared during
     * construction, requiring the field to be initialized during setup. Therefore, it is necessary
     * to verify whether this field is null before using it.
     */
    @Nullable
    private BiConsumer<TieredStorageSubpartitionId, List<Buffer>> accumulatedBufferFlusher;

    /** Whether the current {@link SortBuffer} is a broadcast sort buffer. */
    private boolean isBroadcastDataBuffer;

    public SortBufferAccumulator(
            int numSubpartitions,
            int numBuffers,
            int bufferSizeBytes,
            TieredStorageMemoryManager storeMemoryManager) {
        this.numSubpartitions = numSubpartitions;
        this.bufferSizeBytes = bufferSizeBytes;
        this.numBuffers = numBuffers;
        this.storeMemoryManager = storeMemoryManager;
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
            boolean isBroadcast)
            throws IOException {
        int targetSubpartition = subpartitionId.getSubpartitionId();
        switchCurrentDataBufferIfNeeded(isBroadcast);
        if (checkNotNull(currentDataBuffer).writeRecord(record, targetSubpartition, dataType)) {
            return;
        }

        // The sort buffer is empty, but we failed to write the record into it, which suggests the
        // record is larger than the sort buffer can hold. So the record is written into multiple
        // buffers directly.
        if (!currentDataBuffer.hasRemaining()) {
            currentDataBuffer.release();
            writeLargeRecord(record, targetSubpartition, dataType);
            return;
        }

        flushDataBuffer(currentDataBuffer);
        currentDataBuffer.release();
        if (record.hasRemaining()) {
            receive(record, subpartitionId, dataType, isBroadcast);
        }
    }

    @Override
    public void close() {
        flushCurrentDataBuffer();
        releaseFreeBuffers();
        if (currentDataBuffer != null) {
            currentDataBuffer.release();
        }
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    private void switchCurrentDataBufferIfNeeded(boolean isBroadcast) {
        if (isBroadcast == isBroadcastDataBuffer
                && currentDataBuffer != null
                && !currentDataBuffer.isReleased()
                && !currentDataBuffer.isFinished()) {
            return;
        }
        isBroadcastDataBuffer = isBroadcast;
        flushCurrentDataBuffer();
        currentDataBuffer = createNewDataBuffer();
    }

    private SortBuffer createNewDataBuffer() {
        requestNetworkBuffers();

        return new SortBuffer(
                freeSegments,
                this::recycleBuffer,
                numSubpartitions,
                bufferSizeBytes,
                numBuffersForSort);
    }

    private void requestGuaranteedBuffers() {
        int effectiveRequiredBuffers = effectiveNumRequestedBuffers();

        while (freeSegments.size() < effectiveRequiredBuffers) {
            BufferBuilder bufferBuilder = storeMemoryManager.requestBufferBlocking(this);
            Buffer buffer = bufferBuilder.createBufferConsumerFromBeginning().build();
            freeSegments.add(checkNotNull(buffer).getMemorySegment());
            if (bufferRecycler == null) {
                bufferRecycler = buffer.getRecycler();
            }
        }
    }

    private void requestNetworkBuffers() {
        requestGuaranteedBuffers();

        // Use the half of the buffers for writing, and the other half for reading
        numBuffersForSort = freeSegments.size() / 2;
    }

    private void flushDataBuffer(SortBuffer sortBuffer) {
        if (sortBuffer == null || sortBuffer.isReleased() || !sortBuffer.hasRemaining()) {
            return;
        }
        sortBuffer.finish();

        do {
            MemorySegment freeSegment = getFreeSegment();
            Pair<Integer, Buffer> bufferAndSubpartitionId = sortBuffer.readBuffer(freeSegment);
            if (bufferAndSubpartitionId == null) {
                if (freeSegment != null) {
                    recycleBuffer(freeSegment);
                }
                break;
            }
            flushBuffer(bufferAndSubpartitionId);
        } while (true);

        releaseFreeBuffers();
        sortBuffer.release();
    }

    private void flushCurrentDataBuffer() {
        if (currentDataBuffer != null) {
            flushDataBuffer(currentDataBuffer);
            currentDataBuffer.release();
            currentDataBuffer = null;
        }
    }

    private void writeLargeRecord(
            ByteBuffer record, int targetSubpartition, Buffer.DataType dataType) {

        checkState(dataType != Buffer.DataType.EVENT_BUFFER);
        while (record.hasRemaining()) {
            int toCopy = Math.min(record.remaining(), bufferSizeBytes);
            MemorySegment writeBuffer = checkNotNull(getFreeSegment());
            writeBuffer.put(0, record, toCopy);

            flushBuffer(
                    Pair.of(
                            targetSubpartition,
                            new NetworkBuffer(
                                    writeBuffer, checkNotNull(bufferRecycler), dataType, toCopy)));
        }

        releaseFreeBuffers();
    }

    private MemorySegment getFreeSegment() {
        MemorySegment freeSegment = freeSegments.poll();
        if (freeSegment == null) {
            BufferBuilder bufferBuilder = storeMemoryManager.requestBufferBlocking(this);
            Buffer buffer = bufferBuilder.createBufferConsumerFromBeginning().build();
            freeSegment = buffer.getMemorySegment();
        }
        return freeSegment;
    }

    private int effectiveNumRequestedBuffers() {
        return Math.min(numSubpartitions, numBuffers);
    }

    private void flushBuffer(Pair<Integer, Buffer> bufferAndSubpartitionId) {
        checkNotNull(accumulatedBufferFlusher)
                .accept(
                        new TieredStorageSubpartitionId(bufferAndSubpartitionId.getLeft()),
                        Collections.singletonList(bufferAndSubpartitionId.getRight()));
    }

    private void releaseFreeBuffers() {
        freeSegments.forEach(this::recycleBuffer);
        freeSegments.clear();
    }

    private void recycleBuffer(MemorySegment memorySegment) {
        checkNotNull(bufferRecycler).recycle(memorySegment);
    }
}
