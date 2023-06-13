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

public class SortBufferAccumulator1 implements BufferAccumulator {
    private static final int NUM_WRITE_BUFFER_BYTES = 4 * 1024 * 1024;

    private static final int EXPECTED_WRITE_BATCH_SIZE = 256;

    private final int numSubpartitions;

    private final int numBuffersOfSortAccumulatorThreshold;

    private final int bufferSize;

    private final TieredStorageMemoryManager storeMemoryManager;

    private int numBuffersForSort;

    private boolean useHashBuffer = false;

    private CacheBuffer broadcastDataBuffer;

    private CacheBuffer unicastDataBuffer;

    private final LinkedList<MemorySegment> freeSegments = new LinkedList<>();

    private BufferRecycler bufferRecycler;

    /**
     * The {@link HashBufferAccumulator}'s accumulated buffer flusher is not prepared during
     * construction, requiring the field to be initialized during setup. Therefore, it is necessary
     * to verify whether this field is null before using it.
     */
    @Nullable
    private BiConsumer<TieredStorageSubpartitionId, List<Buffer>> accumulatedBufferFlusher;

    public SortBufferAccumulator1(
            TieredStoragePartitionId partitionId,
            int numSubpartitions,
            int numBuffersOfSortAccumulatorThreshold,
            int bufferSize,
            TieredStorageMemoryManager storeMemoryManager,
            TieredStorageResourceRegistry resourceRegistry) {
        this.numSubpartitions = numSubpartitions;
        this.bufferSize = bufferSize;
        this.numBuffersOfSortAccumulatorThreshold = numBuffersOfSortAccumulatorThreshold;
        this.storeMemoryManager = storeMemoryManager;
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
            flushUnicastDataBuffer();
            flushBroadcastDataBuffer();
        }
        int targetSubpartition = subpartitionId.getSubpartitionId();
        CacheBuffer dataBuffer = isBroadcast ? getBroadcastDataBuffer() : getUnicastDataBuffer();
        if (!dataBuffer.append(record, targetSubpartition, dataType)) {
            if (isEndOfPartition) {
                flushDataBuffer(dataBuffer, isBroadcast, true);
            }
            return;
        }

        if (!dataBuffer.hasRemaining()) {
            dataBuffer.release();
            writeLargeRecord(record, targetSubpartition, dataType, isBroadcast);
            if (isEndOfPartition) {
                flushDataBuffer(dataBuffer, isBroadcast, true);
            }
            return;
        }

        flushDataBuffer(dataBuffer, isBroadcast, isEndOfPartition);
        dataBuffer.release();
        if (record.hasRemaining()) {
            receive(record, subpartitionId, dataType, isBroadcast, isEndOfPartition);
        }
    }

    @Override
    public void close() {}

    public void release() {
        releaseFreeBuffers();
        // the close method will always be called by the task thread, so there is need to make
        // the sort buffer fields volatile and visible to the cancel thread intermediately
        releaseDataBuffer(unicastDataBuffer);
        releaseDataBuffer(broadcastDataBuffer);
    }

    private CacheBuffer getUnicastDataBuffer() throws IOException {
        flushBroadcastDataBuffer();

        if (unicastDataBuffer != null
                && !unicastDataBuffer.isFinished()
                && !unicastDataBuffer.isReleased()) {
            return unicastDataBuffer;
        }

        unicastDataBuffer = createNewDataBuffer();
        return unicastDataBuffer;
    }

    private CacheBuffer getBroadcastDataBuffer() throws IOException {
        flushUnicastDataBuffer();

        if (broadcastDataBuffer != null
                && !broadcastDataBuffer.isFinished()
                && !broadcastDataBuffer.isReleased()) {
            return broadcastDataBuffer;
        }

        broadcastDataBuffer = createNewDataBuffer();
        return broadcastDataBuffer;
    }

    private CacheBuffer createNewDataBuffer() throws IOException {
        requestNetworkBuffers();

        return new SortBasedCacheBuffer(
                freeSegments,
                this::recycleBuffer,
                numSubpartitions,
                bufferSize,
                numBuffersForSort,
                null);
    }

    private void requestGuaranteedBuffers() throws IOException {

        //        int effectiveRequiredBuffers = Math.min(EXPECTED_WRITE_BATCH_SIZE,
        // numSubpartitions + 8);
        int effectiveRequiredBuffers =
                Math.min(numSubpartitions + 1, numBuffersOfSortAccumulatorThreshold);

        while (freeSegments.size() < effectiveRequiredBuffers) {
            BufferBuilder bufferBuilder = storeMemoryManager.requestBufferBlocking(this);
            Buffer buffer = bufferBuilder.createBufferConsumerFromBeginning().build();
            freeSegments.add(checkNotNull(buffer).getMemorySegment());
            if (bufferRecycler == null) {
                bufferRecycler = buffer.getRecycler();
            }
        }
    }

    private void requestNetworkBuffers() throws IOException {
        requestGuaranteedBuffers();

        useHashBuffer = false;
        int numWriteBuffers = 0;
        if (freeSegments.size() > numSubpartitions) {
            useHashBuffer = false;
        } else if (bufferSize >= NUM_WRITE_BUFFER_BYTES) {
            numWriteBuffers = 1;
        } else {
            numWriteBuffers =
                    Math.min(EXPECTED_WRITE_BATCH_SIZE, NUM_WRITE_BUFFER_BYTES / bufferSize);
        }
        numWriteBuffers = Math.min(freeSegments.size() / 2, numWriteBuffers);
        numBuffersForSort = freeSegments.size() - numWriteBuffers;
    }

    private void flushDataBuffer(
            CacheBuffer dataBuffer, boolean isBroadcast, boolean isEndOfPartition) {
        if (dataBuffer == null || dataBuffer.isReleased() || !dataBuffer.hasRemaining()) {
            return;
        }
        dataBuffer.finish();

        do {
            MemorySegment freeSegment = useHashBuffer ? null : getFreeSegment();
            MemorySegmentAndChannel memorySegmentAndChannel = dataBuffer.getNextBuffer(freeSegment);
            if (memorySegmentAndChannel == null) {
                if (freeSegment != null) {
                    recycleBuffer(freeSegment);
                }
                break;
            }
            addFinishedBuffer(memorySegmentAndChannel, isBroadcast, isEndOfPartition);
        } while (true);

        releaseFreeBuffers();
        dataBuffer.release();
    }

    private void flushBroadcastDataBuffer() {
        if (broadcastDataBuffer != null) {
            flushDataBuffer(broadcastDataBuffer, true, false);
            broadcastDataBuffer.release();
            broadcastDataBuffer = null;
        }
    }

    private void flushUnicastDataBuffer() {
        if (unicastDataBuffer != null) {
            flushDataBuffer(unicastDataBuffer, false, false);
            unicastDataBuffer.release();
            unicastDataBuffer = null;
        }
    }

    private void writeLargeRecord(
            ByteBuffer record,
            int targetSubpartition,
            Buffer.DataType dataType,
            boolean isBroadcast) {

        checkState(dataType != Buffer.DataType.EVENT_BUFFER);
        while (record.hasRemaining()) {
            int toCopy = Math.min(record.remaining(), bufferSize);
            MemorySegment writeBuffer = checkNotNull(getFreeSegment());
            writeBuffer.put(0, record, toCopy);

            MemorySegmentAndChannel memorySegmentAndChannel =
                    new MemorySegmentAndChannel(
                            writeBuffer, targetSubpartition, dataType, toCopy, true);
            addFinishedBuffer(memorySegmentAndChannel, isBroadcast, false);
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

    private void releaseDataBuffer(CacheBuffer dataBuffer) {
        if (dataBuffer != null) {
            dataBuffer.release();
        }
    }

    private void addFinishedBuffer(
            MemorySegmentAndChannel bufferWithChannel,
            boolean isBroadcast,
            boolean isEndOfPartition) {
        //        finishedBufferListener.accept(
        //                new CachedBufferContext(
        //                        Collections.singletonList(bufferWithChannel),
        //                        isBroadcast,
        //                        isEndOfPartition));
        NetworkBuffer networkBuffer =
                new NetworkBuffer(
                        bufferWithChannel.getBuffer(),
                        this::recycleBuffer,
                        bufferWithChannel.getDataType(),
                        bufferWithChannel.getDataSize());
        accumulatedBufferFlusher.accept(
                new TieredStorageSubpartitionId(bufferWithChannel.getChannelIndex()),
                Collections.singletonList(networkBuffer));
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

    private void releaseResources() {
        releaseFreeBuffers();
        // the close method will always be called by the task thread, so there is need to make
        // the sort buffer fields volatile and visible to the cancel thread intermediately
        releaseDataBuffer(unicastDataBuffer);
        releaseDataBuffer(broadcastDataBuffer);
    }
}
