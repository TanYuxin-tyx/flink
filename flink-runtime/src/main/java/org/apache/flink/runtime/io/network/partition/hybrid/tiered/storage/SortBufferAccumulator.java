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

/** . */
public class SortBufferAccumulator implements BufferAccumulator {
    private static final int NUM_WRITE_BUFFER_BYTES = 4 * 1024 * 1024;

    private final int numSubpartitions;

    private final int numBuffersOfSortAccumulatorThreshold;

    private final int bufferSize;

    private final TieredStorageMemoryManager storeMemoryManager;

    private int numBuffersForSort;

    private SortBufferContainer broadcastDataBuffer;

    private SortBufferContainer unicastDataBuffer;

    private final LinkedList<MemorySegment> freeSegments = new LinkedList<>();

    private BufferRecycler bufferRecycler;

    /**
     * The {@link HashBufferAccumulator}'s accumulated buffer flusher is not prepared during
     * construction, requiring the field to be initialized during setup. Therefore, it is necessary
     * to verify whether this field is null before using it.
     */
    @Nullable
    private BiConsumer<TieredStorageSubpartitionId, List<Buffer>> accumulatedBufferFlusher;

    public SortBufferAccumulator(
            int numSubpartitions,
            int numBuffersOfSortAccumulatorThreshold,
            int bufferSize,
            TieredStorageMemoryManager storeMemoryManager) {
        this.numSubpartitions = numSubpartitions;
        this.bufferSize = bufferSize;
        this.numBuffersOfSortAccumulatorThreshold = numBuffersOfSortAccumulatorThreshold;
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
            boolean isBroadcast,
            boolean isEndOfPartition)
            throws IOException {
        if (isEndOfPartition) {
            flushUnicastDataBuffer();
            flushBroadcastDataBuffer();
        }
        int targetSubpartition = subpartitionId.getSubpartitionId();
        SortBufferContainer dataBuffer =
                isBroadcast ? getBroadcastDataBuffer() : getUnicastDataBuffer();
        if (!dataBuffer.append(record, targetSubpartition, dataType)) {
            if (isEndOfPartition) {
                flushDataBuffer(dataBuffer);
            }
            return;
        }

        if (!dataBuffer.hasRemaining()) {
            dataBuffer.release();
            writeLargeRecord(record, targetSubpartition, dataType);
            if (isEndOfPartition) {
                flushDataBuffer(dataBuffer);
            }
            return;
        }

        flushDataBuffer(dataBuffer);
        dataBuffer.release();
        if (record.hasRemaining()) {
            receive(record, subpartitionId, dataType, isBroadcast, isEndOfPartition);
        }
    }

    @Override
    public void close() {
        releaseFreeBuffers();
        releaseDataBuffer(unicastDataBuffer);
        releaseDataBuffer(broadcastDataBuffer);
    }

    public void release() {
        releaseFreeBuffers();
        // the close method will always be called by the task thread, so there is need to make
        // the sort buffer fields volatile and visible to the cancel thread intermediately
        releaseDataBuffer(unicastDataBuffer);
        releaseDataBuffer(broadcastDataBuffer);
    }

    private SortBufferContainer getUnicastDataBuffer() {
        flushBroadcastDataBuffer();

        if (unicastDataBuffer != null
                && !unicastDataBuffer.isFinished()
                && !unicastDataBuffer.isReleased()) {
            return unicastDataBuffer;
        }

        unicastDataBuffer = createNewDataBuffer();
        return unicastDataBuffer;
    }

    private SortBufferContainer getBroadcastDataBuffer() {
        flushUnicastDataBuffer();

        if (broadcastDataBuffer != null
                && !broadcastDataBuffer.isFinished()
                && !broadcastDataBuffer.isReleased()) {
            return broadcastDataBuffer;
        }

        broadcastDataBuffer = createNewDataBuffer();
        return broadcastDataBuffer;
    }

    private SortBufferContainer createNewDataBuffer() {
        requestNetworkBuffers();

        return new SortBufferContainer(
                freeSegments,
                this::recycleBuffer,
                numSubpartitions,
                bufferSize,
                numBuffersForSort,
                null);
    }

    private void requestGuaranteedBuffers() {
        int effectiveRequiredBuffers = effectiveRequestedBuffers();

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

        int numWriteBuffers;
        if (bufferSize >= NUM_WRITE_BUFFER_BYTES) {
            numWriteBuffers = 1;
        } else {
            numWriteBuffers =
                    Math.min(effectiveRequestedBuffers() / 2, NUM_WRITE_BUFFER_BYTES / bufferSize);
        }
        numWriteBuffers = Math.min(freeSegments.size() / 2, numWriteBuffers);
        numBuffersForSort = freeSegments.size() - numWriteBuffers;
    }

    private void flushDataBuffer(SortBufferContainer dataBuffer) {
        if (dataBuffer == null || dataBuffer.isReleased() || !dataBuffer.hasRemaining()) {
            return;
        }
        dataBuffer.finish();

        do {
            MemorySegment freeSegment = getFreeSegment();
            Pair<Integer, Buffer> bufferAndChannel = dataBuffer.getNextBuffer(freeSegment);
            if (bufferAndChannel == null) {
                if (freeSegment != null) {
                    recycleBuffer(freeSegment);
                }
                break;
            }
            addFinishedBuffer(bufferAndChannel);
        } while (true);

        releaseFreeBuffers();
        dataBuffer.release();
    }

    private void flushBroadcastDataBuffer() {
        if (broadcastDataBuffer != null) {
            flushDataBuffer(broadcastDataBuffer);
            broadcastDataBuffer.release();
            broadcastDataBuffer = null;
        }
    }

    private void flushUnicastDataBuffer() {
        if (unicastDataBuffer != null) {
            flushDataBuffer(unicastDataBuffer);
            unicastDataBuffer.release();
            unicastDataBuffer = null;
        }
    }

    private void writeLargeRecord(
            ByteBuffer record, int targetSubpartition, Buffer.DataType dataType) {

        checkState(dataType != Buffer.DataType.EVENT_BUFFER);
        while (record.hasRemaining()) {
            int toCopy = Math.min(record.remaining(), bufferSize);
            MemorySegment writeBuffer = checkNotNull(getFreeSegment());
            writeBuffer.put(0, record, toCopy);

            addFinishedBuffer(
                    Pair.of(
                            targetSubpartition,
                            new NetworkBuffer(writeBuffer, bufferRecycler, dataType, toCopy)));
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

    private int effectiveRequestedBuffers() {
        return Math.min(numSubpartitions + 1, numBuffersOfSortAccumulatorThreshold);
    }

    private void releaseDataBuffer(SortBufferContainer dataBuffer) {
        if (dataBuffer != null) {
            dataBuffer.release();
        }
    }

    private void addFinishedBuffer(Pair<Integer, Buffer> bufferAndChannel) {
        checkNotNull(accumulatedBufferFlusher)
                .accept(
                        new TieredStorageSubpartitionId(bufferAndChannel.getLeft()),
                        Collections.singletonList(bufferAndChannel.getRight()));
    }

    private void releaseFreeBuffers() {
        if (storeMemoryManager != null) {
            freeSegments.forEach(this::recycleBuffer);
            freeSegments.clear();
        }
    }

    private void recycleBuffer(MemorySegment memorySegment) {
        bufferRecycler.recycle(memorySegment);
    }
}
