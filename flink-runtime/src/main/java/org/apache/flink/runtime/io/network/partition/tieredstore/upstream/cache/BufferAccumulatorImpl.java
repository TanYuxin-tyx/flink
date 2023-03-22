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

package org.apache.flink.runtime.io.network.partition.tieredstore.upstream.cache;

import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.BufferWithChannel;
import org.apache.flink.runtime.io.network.partition.DataBuffer;
import org.apache.flink.runtime.io.network.partition.HashBasedDataBuffer;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.TieredStoreMode;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreMemoryManager;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedList;
import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class BufferAccumulatorImpl implements BufferAccumulator {

    private static final int NUM_WRITE_BUFFER_BYTES = 4 * 1024 * 1024;

    private static final int EXPECTED_WRITE_BATCH_SIZE = 128;

    private final int numSubpartitions;

    private final int bufferSize;

    private final TieredStoreMemoryManager storeMemoryManager;

    private final BufferCompressor bufferCompressor;

    private final Consumer<CachedBufferContext> finishedBufferListener;

    private int numBuffersForSort;

    private boolean useHashBuffer = false;

    private DataBuffer broadcastDataBuffer;

    private DataBuffer unicastDataBuffer;

    private final LinkedList<MemorySegment> freeSegments = new LinkedList<>();

    public BufferAccumulatorImpl(
            int numSubpartitions,
            int bufferSize,
            @Nullable BufferCompressor bufferCompressor,
            TieredStoreMemoryManager storeMemoryManager,
            Consumer<CachedBufferContext> finishedBufferListener) {
        this.numSubpartitions = numSubpartitions;
        this.bufferSize = bufferSize;
        this.storeMemoryManager = storeMemoryManager;
        this.bufferCompressor = bufferCompressor;
        this.finishedBufferListener = finishedBufferListener;
    }

    @Override
    public void emit(
            ByteBuffer record,
            int targetSubpartition,
            Buffer.DataType dataType,
            boolean isBroadcast,
            boolean isEndOfPartition)
            throws IOException {
        DataBuffer dataBuffer = isBroadcast ? getBroadcastDataBuffer() : getUnicastDataBuffer();
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
            emit(record, targetSubpartition, dataType, isBroadcast, isEndOfPartition);
        }
    }

    @Override
    public void close() {
        releaseFreeBuffers();
        // the close method will always be called by the task thread, so there is need to make
        // the sort buffer fields volatile and visible to the cancel thread intermediately
        releaseDataBuffer(unicastDataBuffer);
        releaseDataBuffer(broadcastDataBuffer);
    }

    private DataBuffer getUnicastDataBuffer() throws IOException {
        flushBroadcastDataBuffer();

        if (unicastDataBuffer != null
                && !unicastDataBuffer.isFinished()
                && !unicastDataBuffer.isReleased()) {
            return unicastDataBuffer;
        }

        unicastDataBuffer = createNewDataBuffer();
        return unicastDataBuffer;
    }

    private DataBuffer getBroadcastDataBuffer() throws IOException {
        flushUnicastDataBuffer();

        if (broadcastDataBuffer != null
                && !broadcastDataBuffer.isFinished()
                && !broadcastDataBuffer.isReleased()) {
            return broadcastDataBuffer;
        }

        broadcastDataBuffer = createNewDataBuffer();
        return broadcastDataBuffer;
    }

    private DataBuffer createNewDataBuffer() throws IOException {
        requestNetworkBuffers();

        if (useHashBuffer) {
            return new HashBasedDataBuffer(
                    freeSegments,
                    this::recycleBuffer,
                    numSubpartitions,
                    bufferSize,
                    numBuffersForSort,
                    null);
        } else {
            return new SortBasedCacheBuffer(
                    freeSegments,
                    this::recycleBuffer,
                    numSubpartitions,
                    bufferSize,
                    numBuffersForSort,
                    null);
        }
    }

    private void requestGuaranteedBuffers() throws IOException {
        int numRequiredBuffer = storeMemoryManager.numRequiredMemorySegments();
        if (numRequiredBuffer < 2) {
            throw new IOException(
                    String.format(
                            "Too few sort buffers, please increase %s.",
                            NettyShuffleEnvironmentOptions.NETWORK_SORT_SHUFFLE_MIN_BUFFERS));
        }

        while (freeSegments.size() < EXPECTED_WRITE_BATCH_SIZE) {
            freeSegments.add(
                    checkNotNull(
                            storeMemoryManager.requestMemorySegmentBlocking(
                                    TieredStoreMode.TieredType.IN_CACHE)));
        }
    }

    private void requestNetworkBuffers() throws IOException {
        requestGuaranteedBuffers();

        useHashBuffer = false;
        int numWriteBuffers = 0;
        if (freeSegments.size() >= numSubpartitions) {
            useHashBuffer = true;
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
            DataBuffer dataBuffer, boolean isBroadcast, boolean isEndOfPartition) {
        if (dataBuffer == null || dataBuffer.isReleased() || !dataBuffer.hasRemaining()) {
            return;
        }
        dataBuffer.finish();

        do {
            BufferWithChannel bufferWithChannel = dataBuffer.getNextBuffer(getFreeSegment());
            if (bufferWithChannel == null) {
                break;
            }
            addFinishedBuffer(
                    compressBufferIfPossible(bufferWithChannel), isBroadcast, isEndOfPartition);
        } while (true);

        releaseFreeBuffers();
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

    private BufferWithChannel compressBufferIfPossible(BufferWithChannel bufferWithChannel) {
        Buffer buffer = bufferWithChannel.getBuffer();
        if (!canBeCompressed(buffer)) {
            return bufferWithChannel;
        }

        buffer = checkNotNull(bufferCompressor).compressToOriginalBuffer(buffer);
        return new BufferWithChannel(buffer, bufferWithChannel.getChannelIndex());
    }

    private void writeLargeRecord(
            ByteBuffer record,
            int targetSubpartition,
            Buffer.DataType dataType,
            boolean isBroadcast) {

        while (record.hasRemaining()) {
            int toCopy = Math.min(record.remaining(), bufferSize);
            MemorySegment writeBuffer = checkNotNull(getFreeSegment());
            writeBuffer.put(0, record, toCopy);

            NetworkBuffer buffer =
                    new NetworkBuffer(writeBuffer, this::recycleBuffer, dataType, toCopy);
            BufferWithChannel bufferWithChannel = new BufferWithChannel(buffer, targetSubpartition);
            addFinishedBuffer(compressBufferIfPossible(bufferWithChannel), isBroadcast, false);
        }

        releaseFreeBuffers();
    }

    private MemorySegment getFreeSegment() {
        //        return checkNotNull(freeSegments.poll());
        MemorySegment freeSegment = freeSegments.poll();
        if (freeSegment == null) {
            freeSegment =
                    storeMemoryManager.requestMemorySegmentBlocking(
                            TieredStoreMode.TieredType.IN_CACHE);
        }
        return freeSegment;
    }

    private void releaseDataBuffer(DataBuffer dataBuffer) {
        if (dataBuffer != null) {
            dataBuffer.release();
        }
    }

    private void addFinishedBuffer(
            BufferWithChannel bufferWithChannel, boolean isBroadcast, boolean isEndOfPartition) {
        finishedBufferListener.accept(
                new CachedBufferContext(
                        bufferWithChannel.getChannelIndex(),
                        Collections.singletonList(bufferWithChannel.getBuffer()),
                        isBroadcast,
                        isEndOfPartition));
    }

    /**
     * Whether the buffer can be compressed or not. Note that event is not compressed because it is
     * usually small and the size can become even larger after compression.
     */
    private boolean canBeCompressed(Buffer buffer) {
        return bufferCompressor != null && buffer.isBuffer() && buffer.readableBytes() > 0;
    }

    private void releaseFreeBuffers() {
        if (storeMemoryManager != null) {
            freeSegments.forEach(this::recycleBuffer);
            freeSegments.clear();
        }
    }

    private void recycleBuffer(MemorySegment memorySegment) {
        storeMemoryManager.recycleBuffer(memorySegment, TieredStoreMode.TieredType.IN_CACHE);
    }
}
