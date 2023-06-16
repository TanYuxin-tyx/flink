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
 * The {@link SubpartitionDiskCacheManager} is responsible for managing the cached buffers in a
 * single subpartition.
 */
public class SubpartitionDiskCacheManager {

    private final int subpartitionId;

    private int finishedBufferIndex;

    // Note that this field can be accessed by multiple threads, so the thread safety should be
    // ensured.
    private final Deque<NettyPayload> allBuffers = new LinkedList<>();

    public SubpartitionDiskCacheManager(int subpartitionId) {
        this.subpartitionId = subpartitionId;
    }

    // ------------------------------------------------------------------------
    //  Called by DiskCacheManager
    // ------------------------------------------------------------------------
    void appendEndOfSegmentEvent(ByteBuffer record, DataType dataType) {
        writeEvent(record, dataType);
    }

    int getFinishedBufferIndex() {
        return finishedBufferIndex;
    }

    void append(Buffer buffer) {
        NettyPayload toAddBuffer =
                NettyPayload.newBuffer(buffer, finishedBufferIndex, subpartitionId);
        addFinishedBuffer(toAddBuffer);
    }

    // Note that allBuffers can be touched by multiple threads.
    List<NettyPayload> getAllBuffers() {
        synchronized (allBuffers) {
            List<NettyPayload> targetBuffers = new ArrayList<>(allBuffers);
            allBuffers.clear();
            return targetBuffers;
        }
    }

    public void release() {
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

        NettyPayload nettyPayload =
                NettyPayload.newBuffer(buffer, finishedBufferIndex, subpartitionId);
        addFinishedBuffer(nettyPayload);
    }

    // Note that allBuffers can be touched by multiple threads.
    private void addFinishedBuffer(NettyPayload nettyPayload) {
        synchronized (allBuffers) {
            finishedBufferIndex++;
            allBuffers.add(nettyPayload);
        }
    }
}
