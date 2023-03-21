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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.SubpartitionDiskCacheManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Optional;
import java.util.Queue;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class SubpartitionCachedBuffer {
    private static final Logger LOG = LoggerFactory.getLogger(SubpartitionDiskCacheManager.class);

    private final int targetChannel;

    private final int bufferSize;

    private final Supplier<BufferBuilder> bufferRequestSupplier;

    private final Consumer<CachedBufferContext> finishedBufferListener;

    // Not guarded by lock because it is expected only accessed from task's main thread.
    private final Queue<BufferBuilder> unfinishedBuffers = new LinkedList<>();

    @Nullable private final BufferCompressor bufferCompressor;

    public SubpartitionCachedBuffer(
            int targetChannel,
            int bufferSize,
            @Nullable BufferCompressor bufferCompressor,
            Consumer<CachedBufferContext> finishedBufferListener,
            Supplier<BufferBuilder> bufferRequestSupplier) {
        this.targetChannel = targetChannel;
        this.bufferSize = bufferSize;
        this.bufferRequestSupplier = bufferRequestSupplier;
        this.finishedBufferListener = finishedBufferListener;
        this.bufferCompressor = bufferCompressor;
    }

    // ------------------------------------------------------------------------
    //  Called by MemoryDataManager
    // ------------------------------------------------------------------------

    public void append(
            ByteBuffer record,
            Buffer.DataType dataType,
            boolean isBroadcast,
            boolean isEndOfPartition)
            throws IOException, InterruptedException {
        if (dataType.isEvent()) {
            writeEvent(record, dataType, isBroadcast, isEndOfPartition);
        } else {
            writeRecord(record, dataType, isBroadcast, isEndOfPartition);
        }
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    private void writeEvent(
            ByteBuffer event,
            Buffer.DataType dataType,
            boolean isBroadcast,
            boolean isEndOfPartition)
            throws IOException {
        checkArgument(dataType.isEvent());

        // each Event must take an exclusive buffer
        finishCurrentWritingBufferIfNotEmpty(isBroadcast);

        // store Events in adhoc heap segments, for network memory efficiency
        MemorySegment data = MemorySegmentFactory.wrap(event.array());
        Buffer buffer =
                new NetworkBuffer(data, FreeingBufferRecycler.INSTANCE, dataType, data.size());
        addFinishedBuffer(buffer, isBroadcast, isEndOfPartition);
    }

    private void writeRecord(
            ByteBuffer record,
            Buffer.DataType dataType,
            boolean isBroadcast,
            boolean isEndOfPartition)
            throws InterruptedException, IOException {
        checkArgument(!dataType.isEvent());

        ensureCapacityForRecord(record);

        writeRecord(record, isBroadcast, isEndOfPartition);
    }

    private void ensureCapacityForRecord(ByteBuffer record) {
        final int numRecordBytes = record.remaining();
        int availableBytes =
                Optional.ofNullable(unfinishedBuffers.peek())
                        .map(
                                currentWritingBuffer ->
                                        currentWritingBuffer.getWritableBytes()
                                                + bufferSize * (unfinishedBuffers.size() - 1))
                        .orElse(0);

        while (availableBytes < numRecordBytes) {
            // request unfinished buffer.
            BufferBuilder bufferBuilder = bufferRequestSupplier.get();
            unfinishedBuffers.add(bufferBuilder);
            availableBytes += bufferSize;
        }
    }

    private void writeRecord(ByteBuffer record, boolean isBroadcast, boolean isEndOfPartition)
            throws IOException {
        while (record.hasRemaining()) {
            BufferBuilder currentWritingBuffer =
                    checkNotNull(
                            unfinishedBuffers.peek(), "Expect enough capacity for the record.");
            currentWritingBuffer.append(record);
            if (currentWritingBuffer.isFull()) {
                finishCurrentWritingBuffer(isBroadcast, isEndOfPartition);
            }
        }
    }

    private void finishCurrentWritingBufferIfNotEmpty(boolean isBroadcast) throws IOException {
        BufferBuilder currentWritingBuffer = unfinishedBuffers.peek();
        if (currentWritingBuffer == null || currentWritingBuffer.getWritableBytes() == bufferSize) {
            return;
        }

        finishCurrentWritingBuffer(isBroadcast, false);
    }

    private void finishCurrentWritingBuffer(boolean isBroadcast, boolean isEndOfPartition)
            throws IOException {
        BufferBuilder currentWritingBuffer = unfinishedBuffers.poll();
        if (currentWritingBuffer == null) {
            return;
        }
        currentWritingBuffer.finish();
        BufferConsumer bufferConsumer = currentWritingBuffer.createBufferConsumerFromBeginning();
        Buffer buffer = bufferConsumer.build();
        currentWritingBuffer.close();
        bufferConsumer.close();
        addFinishedBuffer(compressBuffersIfPossible(buffer), isBroadcast, isEndOfPartition);
    }

    private Buffer compressBuffersIfPossible(Buffer buffer) {
        if (!canBeCompressed(buffer)) {
            return buffer;
        }
        return checkNotNull(bufferCompressor).compressToOriginalBuffer(buffer);
    }

    /**
     * Whether the buffer can be compressed or not. Note that event is not compressed because it is
     * usually small and the size can become even larger after compression.
     */
    private boolean canBeCompressed(Buffer buffer) {
        return bufferCompressor != null && buffer.isBuffer() && buffer.readableBytes() > 0;
    }

    @SuppressWarnings("FieldAccessNotGuarded")
    // Note that: callWithLock ensure that code block guarded by resultPartitionReadLock and
    // subpartitionLock.
    private void addFinishedBuffer(Buffer buffer, boolean isBroadcast, boolean isEndOfPartition) {
        finishedBufferListener.accept(
                new CachedBufferContext(
                        targetChannel,
                        Collections.singletonList(buffer),
                        isBroadcast,
                        isEndOfPartition));
    }
}
