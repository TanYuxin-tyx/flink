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
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.function.BiConsumer;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class SubpartitionCachedBuffer {

    private final TieredStorageSubpartitionId subpartitionId;

    private final int bufferSize;

    private final TieredStorageMemoryManager storageMemoryManager;

    private final CacheFlushManager cacheFlushManager;

    private BiConsumer<TieredStorageSubpartitionId, List<Buffer>> bufferFlusher;

    // Not guarded by lock because it is expected only accessed from task's main thread.
    private final Queue<BufferBuilder> unfinishedBuffers = new LinkedList<>();

    public SubpartitionCachedBuffer(
            TieredStorageSubpartitionId subpartitionId,
            int bufferSize,
            TieredStorageMemoryManager storageMemoryManager,
            CacheFlushManager cacheFlushManager) {
        this.subpartitionId = subpartitionId;
        this.bufferSize = bufferSize;
        this.storageMemoryManager = storageMemoryManager;
        this.cacheFlushManager = cacheFlushManager;
    }

    public void setup(BiConsumer<TieredStorageSubpartitionId, List<Buffer>> bufferFlusher) {
        this.bufferFlusher = bufferFlusher;
    }

    // ------------------------------------------------------------------------
    //  Called by MemoryDataManager
    // ------------------------------------------------------------------------

    public void append(ByteBuffer record, Buffer.DataType dataType)
            throws IOException, InterruptedException {
        if (dataType.isEvent()) {
            writeEvent(record, dataType);
        } else {
            writeRecord(record, dataType);
        }
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    private void writeEvent(ByteBuffer event, Buffer.DataType dataType) {
        checkArgument(dataType.isEvent());

        // each Event must take an exclusive buffer
        finishCurrentWritingBufferIfNotEmpty();

        // store Events in adhoc heap segments, for network memory efficiency
        MemorySegment data = MemorySegmentFactory.wrap(event.array());
        addFinishedBuffer(
                // Note that the buffer recycler should be replaced before writing to the tiers.
                new NetworkBuffer(data, FreeingBufferRecycler.INSTANCE, dataType, data.size()));
    }

    private void writeRecord(ByteBuffer record, Buffer.DataType dataType)
            throws InterruptedException {
        checkArgument(!dataType.isEvent());

        ensureCapacityForRecord(record);

        writeRecord(record);
    }

    private void ensureCapacityForRecord(ByteBuffer record) throws InterruptedException {
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
            BufferBuilder bufferBuilder = requestBufferFromPool();
            unfinishedBuffers.add(bufferBuilder);
            availableBytes += bufferSize;
        }
    }

    private void writeRecord(ByteBuffer record) {
        while (record.hasRemaining()) {
            BufferBuilder currentWritingBuffer =
                    checkNotNull(
                            unfinishedBuffers.peek(), "Expect enough capacity for the record.");
            currentWritingBuffer.append(record);
            if (currentWritingBuffer.isFull()) {
                finishCurrentWritingBuffer();
            }
        }
    }

    private void finishCurrentWritingBufferIfNotEmpty() {
        BufferBuilder currentWritingBuffer = unfinishedBuffers.peek();
        if (currentWritingBuffer == null || currentWritingBuffer.getWritableBytes() == bufferSize) {
            return;
        }

        finishCurrentWritingBuffer();
    }

    private void finishCurrentWritingBuffer() {
        BufferBuilder currentWritingBuffer = unfinishedBuffers.poll();
        if (currentWritingBuffer == null) {
            return;
        }
        currentWritingBuffer.finish();
        BufferConsumer bufferConsumer = currentWritingBuffer.createBufferConsumerFromBeginning();
        Buffer buffer = bufferConsumer.build();
        currentWritingBuffer.close();
        bufferConsumer.close();
        addFinishedBuffer(
                new NetworkBuffer(
                        buffer.getMemorySegment(),
                        // Note that the buffer recycler should be replaced before writing to the
                        // tiers.
                        FreeingBufferRecycler.INSTANCE,
                        buffer.getDataType(),
                        buffer.getSize()));
    }

    @SuppressWarnings("FieldAccessNotGuarded")
    // Note that: callWithLock ensure that code block guarded by resultPartitionReadLock and
    // subpartitionLock.
    private void addFinishedBuffer(Buffer finishedBuffer) {
        bufferFlusher.accept(subpartitionId, Collections.singletonList(finishedBuffer));
    }

    public BufferBuilder requestBufferFromPool() {
        MemorySegment segment = storageMemoryManager.requestBufferInAccumulator();
        cacheFlushManager.checkNeedTriggerFlushCachedBuffers();
        return new BufferBuilder(segment, this::recycleBuffer);
    }

    private void recycleBuffer(MemorySegment buffer) {
        storageMemoryManager.recycleBufferInAccumulator(buffer);
    }
}
