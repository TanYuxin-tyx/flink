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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.disk;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.Buffer.DataType;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyPayload;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The {@link SubpartitionDiskCacheManager} is responsible to manage the cached buffers in a single
 * subpartition.
 */
class SubpartitionDiskCacheManager {

    /** The segment id of the {@link SubpartitionDiskCacheManager}. */
    private final int subpartitionId;

    // Note that this field can be accessed by the task thread or the write IO thread, so the thread
    // safety should be ensured.
    private final Deque<NettyPayload> allBuffers = new LinkedList<>();

    /**
     * Record the buffer index in the {@link SubpartitionDiskCacheManager}. Each time a new buffer
     * is added to the {@code allBuffers}, this field is increased by one.
     */
    private int bufferIndex;

    /**
     * Record the segment id that is writing to. Each time when the segment is finished, this filed
     * is increased by one.
     */
    private int segmentIndex;

    SubpartitionDiskCacheManager(int subpartitionId) {
        this.subpartitionId = subpartitionId;
    }

    // ------------------------------------------------------------------------
    //  Called by DiskCacheManager
    // ------------------------------------------------------------------------
    void appendEndOfSegmentEvent(ByteBuffer record, DataType dataType) {
        writeEvent(record, dataType);
        segmentIndex++;
    }

    int getBufferIndex() {
        return bufferIndex;
    }

    int getSegmentIndex() {
        return segmentIndex;
    }

    void append(Buffer buffer) {
        addBuffer(NettyPayload.newBuffer(buffer, bufferIndex, subpartitionId));
    }

    // Note that allBuffers can be touched by multiple threads.
    List<NettyPayload> removeAllBuffers() {
        synchronized (allBuffers) {
            List<NettyPayload> targetBuffers = new ArrayList<>(allBuffers);
            allBuffers.clear();
            return targetBuffers;
        }
    }

    public void release() {
        recycleBuffers();
        checkState(allBuffers.isEmpty(), "Leaking buffers.");
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    private void writeEvent(ByteBuffer event, DataType dataType) {
        checkArgument(dataType.isEvent());

        MemorySegment data = MemorySegmentFactory.wrap(event.array());
        Buffer buffer =
                new NetworkBuffer(data, FreeingBufferRecycler.INSTANCE, dataType, data.size());

        NettyPayload nettyPayload = NettyPayload.newBuffer(buffer, bufferIndex, subpartitionId);
        addBuffer(nettyPayload);
    }

    // Note that allBuffers can be touched by multiple threads.
    private void addBuffer(NettyPayload nettyPayload) {
        synchronized (allBuffers) {
            bufferIndex++;
            allBuffers.add(nettyPayload);
        }
    }

    private void recycleBuffers() {
        for (NettyPayload nettyPayload : allBuffers) {
            Buffer buffer = nettyPayload.getBuffer().get();
            if (!buffer.isRecycled()) {
                buffer.recycleBuffer();
            }
        }
        allBuffers.clear();
    }
}
