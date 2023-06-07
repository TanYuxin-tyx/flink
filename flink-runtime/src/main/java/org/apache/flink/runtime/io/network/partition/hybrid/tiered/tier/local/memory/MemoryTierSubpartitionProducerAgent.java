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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.local.memory;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.Buffer.DataType;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyServiceImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.NettyPayload;

import java.nio.ByteBuffer;

import static org.apache.flink.util.Preconditions.checkArgument;

/** TODO. */
public class MemoryTierSubpartitionProducerAgent {

    private final int subpartitionId;

    private int finishedBufferIndex;

    private final TieredStorageNettyService nettyService;

    private NettyConnectionWriter nettyConnectionWriter;

    public MemoryTierSubpartitionProducerAgent(
            int subpartitionId, TieredStorageNettyService nettyService) {
        this.subpartitionId = subpartitionId;
        this.nettyService = nettyService;
    }

    // ------------------------------------------------------------------------
    //  Called by MemoryTierProducerAgent
    // ------------------------------------------------------------------------

    public void registerNettyService(NettyConnectionWriter nettyConnectionWriter) {
        this.nettyConnectionWriter = nettyConnectionWriter;
    }

    public void appendSegmentEvent(ByteBuffer record, DataType dataType) {
        writeEvent(record, dataType);
    }

    public void release() {
        if (nettyConnectionWriter != null) {
            nettyConnectionWriter.close();
        }
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

    void addFinishedBuffer(Buffer buffer) {
        NettyPayload toAddBuffer =
                NettyPayload.newBuffer(buffer, finishedBufferIndex, subpartitionId);
        addFinishedBuffer(toAddBuffer);
    }

    void addSegmentBufferContext(int segmentId) {
        NettyPayload segmentNettyPayload = NettyPayload.newSegment(segmentId);
        addFinishedBuffer(segmentNettyPayload);
    }

    private void addFinishedBuffer(NettyPayload nettyPayload) {
        finishedBufferIndex++;
        nettyConnectionWriter.writeBuffer(nettyPayload);
        if (nettyConnectionWriter.numQueuedBuffers() <= 1) {
            ((TieredStorageNettyServiceImpl) nettyService)
                    .notifyResultSubpartitionViewSendBuffer(
                            nettyConnectionWriter.getNettyConnectionId());
        }
    }
}
