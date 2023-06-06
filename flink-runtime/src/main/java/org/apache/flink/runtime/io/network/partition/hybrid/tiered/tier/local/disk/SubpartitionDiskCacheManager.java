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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.local.disk;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.Buffer.DataType;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.BufferContext;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkArgument;

/** This class is responsible for managing the data in a single subpartition. */
public class SubpartitionDiskCacheManager {

    private final int targetChannel;

    private final int bufferSize;

    // Not guarded by lock because it is expected only accessed from task's main thread.
    private final Queue<BufferBuilder> unfinishedBuffers = new LinkedList<>();

    // Not guarded by lock because it is expected only accessed from task's main thread.
    private int finishedBufferIndex;

    private final Deque<BufferContext> allBuffers = new LinkedList<>();

    private final Set<Integer> lastBufferIndexOfSegments;

    public SubpartitionDiskCacheManager(int targetChannel, int bufferSize) {
        this.targetChannel = targetChannel;
        this.bufferSize = bufferSize;
        this.lastBufferIndexOfSegments = new HashSet<>();
    }

    // ------------------------------------------------------------------------
    //  Called by MemoryDataManager
    // ------------------------------------------------------------------------

    public void appendSegmentEvent(ByteBuffer record, DataType dataType) {
        writeEvent(record, dataType);
    }

    public int getFinishedBufferIndex() {
        return finishedBufferIndex;
    }

    public void append(Buffer buffer) {
        BufferContext toAddBuffer = new BufferContext(buffer, finishedBufferIndex, targetChannel);
        addFinishedBuffer(toAddBuffer);
    }

    // Note that: callWithLock ensure that code block guarded by resultPartitionReadLock and
    // subpartitionLock.
    public List<BufferContext> getBuffersSatisfyStatus() {
        synchronized (allBuffers) {
            List<BufferContext> targetBuffers = new ArrayList<>(allBuffers);
            allBuffers.clear();
            return targetBuffers;
        }
    }

    Set<Integer> getLastBufferIndexOfSegments() {
        return lastBufferIndexOfSegments;
    }

    public void release() {
        lastBufferIndexOfSegments.clear();
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    private void writeEvent(ByteBuffer event, DataType dataType) {
        checkArgument(dataType.isEvent());

        // each Event must take an exclusive buffer
        finishCurrentWritingBufferIfNotEmpty();

        // store Events in adhoc heap segments, for network memory efficiency
        MemorySegment data = MemorySegmentFactory.wrap(event.array());
        Buffer buffer =
                new NetworkBuffer(data, FreeingBufferRecycler.INSTANCE, dataType, data.size());

        BufferContext bufferContext = new BufferContext(buffer, finishedBufferIndex, targetChannel);
        addFinishedBuffer(bufferContext);
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
        BufferContext bufferContext = new BufferContext(buffer, finishedBufferIndex, targetChannel);
        addFinishedBuffer(bufferContext);
    }

    @SuppressWarnings("FieldAccessNotGuarded")
    // Note that: callWithLock ensure that code block guarded by resultPartitionReadLock and
    // subpartitionLock.
    private void addFinishedBuffer(BufferContext bufferContext) {
        synchronized (allBuffers) {
            finishedBufferIndex++;
            allBuffers.add(bufferContext);
        }
    }
}
