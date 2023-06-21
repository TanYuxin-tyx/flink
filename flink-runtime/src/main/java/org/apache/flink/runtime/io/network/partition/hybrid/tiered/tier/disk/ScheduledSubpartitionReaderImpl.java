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
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.PartitionFileIndex;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.PartitionFileReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyPayload;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyServiceImpl;

import java.io.IOException;
import java.util.Map;
import java.util.Queue;

import static org.apache.flink.util.Preconditions.checkArgument;

/** The implementation of {@link ScheduledSubpartitionReaderImpl}. */
public class ScheduledSubpartitionReaderImpl implements ScheduledSubpartitionReader {

    private final NettyConnectionId nettyServiceWriterId;

    private final TieredStoragePartitionId partitionId;

    private final TieredStorageSubpartitionId subpartitionId;

    private final int maxBufferReadAhead;

    private final TieredStorageNettyService nettyService;

    private final NettyConnectionWriter nettyConnectionWriter;

    private final Map<Integer, Integer> segmentIdRecorder;

    private final PartitionFileReader partitionFileReader;

    private int nextBufferIndex = 0;

    private boolean isFailed;

    public ScheduledSubpartitionReaderImpl(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int maxBufferReadAhead,
            NettyConnectionWriter nettyConnectionWriter,
            TieredStorageNettyService nettyService,
            Map<Integer, Integer> segmentIdRecorder,
            PartitionFileReader partitionFileReader,
            PartitionFileIndex dataIndex) {
        this.partitionId = partitionId;
        this.subpartitionId = subpartitionId;
        this.nettyServiceWriterId = nettyConnectionWriter.getNettyConnectionId();
        this.maxBufferReadAhead = maxBufferReadAhead;
        this.nettyService = nettyService;
        this.nettyConnectionWriter = nettyConnectionWriter;
        this.segmentIdRecorder = segmentIdRecorder;
        this.partitionFileReader = partitionFileReader;
    }

    @Override
    public void loadDiskDataToBuffers(Queue<MemorySegment> buffers, BufferRecycler recycler)
            throws IOException {

        if (isFailed) {
            throw new IOException(
                    "The scheduled subpartition reader for "
                            + subpartitionId
                            + " has already been failed.");
        }

        // If the number of written but unsent buffers achieves the limited value, skip this time.
        if (nettyConnectionWriter.numQueuedBuffers() >= maxBufferReadAhead) {
            return;
        }
        int numLoaded = 0;
        while (!buffers.isEmpty()
                && nettyConnectionWriter.numQueuedBuffers() < maxBufferReadAhead) {
            MemorySegment memorySegment = buffers.poll();
            Buffer buffer;
            try {
                if ((buffer =
                                partitionFileReader.readBuffer(
                                        partitionId,
                                        subpartitionId,
                                        -1,
                                        nextBufferIndex,
                                        memorySegment,
                                        recycler))
                        == null) {
                    buffers.add(memorySegment);
                    break;
                }
            } catch (Throwable throwable) {
                buffers.add(memorySegment);
                throw throwable;
            }
            NettyPayload nettyPayload =
                    NettyPayload.newBuffer(
                            buffer, nextBufferIndex++, subpartitionId.getSubpartitionId());
            Integer segmentId = segmentIdRecorder.get(nettyPayload.getBufferIndex());
            if (segmentId != null) {
                nettyConnectionWriter.writeBuffer(NettyPayload.newSegment(segmentId));
                ((TieredStorageNettyServiceImpl) nettyService)
                        .notifyResultSubpartitionViewSendBuffer(nettyServiceWriterId);
                ++numLoaded;
            }
            nettyConnectionWriter.writeBuffer(nettyPayload);
            ++numLoaded;
        }
        if (nettyConnectionWriter.numQueuedBuffers() <= numLoaded) {
            ((TieredStorageNettyServiceImpl) nettyService)
                    .notifyResultSubpartitionViewSendBuffer(nettyServiceWriterId);
        }
    }

    @Override
    public int compareTo(ScheduledSubpartitionReader reader) {
        checkArgument(reader != null);
        return Long.compare(getReadingFileOffset(), reader.getReadingFileOffset());
    }

    @Override
    public long getReadingFileOffset() {
        return partitionFileReader.getReadingFileOffset(
                partitionId, subpartitionId, -1, nextBufferIndex);
    }

    @Override
    public void failReader(Throwable failureCause) {
        if (isFailed) {
            return;
        }
        isFailed = true;
        nettyConnectionWriter.close(failureCause);
        ((TieredStorageNettyServiceImpl) nettyService)
                .notifyResultSubpartitionViewSendBuffer(nettyServiceWriterId);
    }
}
