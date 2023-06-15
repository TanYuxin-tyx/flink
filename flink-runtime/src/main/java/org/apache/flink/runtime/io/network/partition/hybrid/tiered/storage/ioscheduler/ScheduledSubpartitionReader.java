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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.ioscheduler;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyPayload;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyServiceImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file.PartitionFileReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.disk.RegionBufferIndexTracker;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/** The implementation of {@link ScheduledSubpartitionReader}. */
public class ScheduledSubpartitionReader implements Comparable<ScheduledSubpartitionReader> {

    private final NettyConnectionId nettyServiceWriterId;

    private final int subpartitionId;

    private final int maxBufferReadAhead;

    private int nextToLoad = 0;

    private boolean isFailed;

    private final TieredStorageNettyService nettyService;

    private final NettyConnectionWriter nettyConnectionWriter;

    private final Map<Integer, Integer> firstBufferContextInSegment;

    private final PartitionFileReader partitionFileReader;

    private final RegionBufferIndexTracker dataIndex;

    private final ReadingProgressRecorder readingProgressRecorder;

    public ScheduledSubpartitionReader(
            int subpartitionId,
            int maxBufferReadAhead,
            NettyConnectionWriter nettyConnectionWriter,
            TieredStorageNettyService nettyService,
            Map<Integer, Integer> firstBufferContextInSegment,
            PartitionFileReader partitionFileReader,
            RegionBufferIndexTracker dataIndex) {
        this.subpartitionId = subpartitionId;
        this.nettyServiceWriterId = nettyConnectionWriter.getNettyConnectionId();
        this.maxBufferReadAhead = maxBufferReadAhead;
        this.nettyService = nettyService;
        this.nettyConnectionWriter = nettyConnectionWriter;
        this.firstBufferContextInSegment = firstBufferContextInSegment;
        this.partitionFileReader = partitionFileReader;
        this.dataIndex = dataIndex;
        this.readingProgressRecorder = new ReadingProgressRecorder(subpartitionId);
    }

    public void readBuffers(Queue<MemorySegment> buffers, BufferRecycler recycler)
            throws IOException {

        if (isFailed) {
            throw new IOException(
                    "The disk tier file reader of subpartition "
                            + subpartitionId
                            + " has already been failed.");
        }

        // If the number of written but unsent buffers achieves the limited value, skip this time.
        if (nettyConnectionWriter.numQueuedBuffers() >= maxBufferReadAhead) {
            return;
        }
        int numRemainingBuffer =
                readingProgressRecorder.getRemainingBuffersInRegion(
                        nextToLoad, nettyServiceWriterId);
        if (numRemainingBuffer == 0) {
            return;
        }

        checkState(numRemainingBuffer > 0);
        int numLoaded = 0;
        while (!buffers.isEmpty()
                && nettyConnectionWriter.numQueuedBuffers() < maxBufferReadAhead
                && numRemainingBuffer-- > 0) {
            MemorySegment segment = buffers.poll();
            Buffer buffer;
            try {
                if ((buffer =
                                partitionFileReader.readBuffer(
                                        subpartitionId,
                                        -1,
                                        readingProgressRecorder.getCurrentFileOffset(),
                                        segment,
                                        recycler))
                        == null) {
                    buffers.add(segment);
                    break;
                }
            } catch (Throwable throwable) {
                buffers.add(segment);
                throw throwable;
            }
            int bufferLength = buffer.readableBytes() + BufferReaderWriterUtil.HEADER_LENGTH;
            readingProgressRecorder.advance(bufferLength);
            NettyPayload nettyPayload =
                    NettyPayload.newBuffer(buffer, nextToLoad++, subpartitionId);
            Integer segmentId = firstBufferContextInSegment.get(nettyPayload.getBufferIndex());
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

    public void fail(Throwable failureCause) {
        if (isFailed) {
            return;
        }
        isFailed = true;
        nettyConnectionWriter.close(failureCause);
        ((TieredStorageNettyServiceImpl) nettyService)
                .notifyResultSubpartitionViewSendBuffer(nettyServiceWriterId);
    }

    @Override
    public int compareTo(ScheduledSubpartitionReader that) {
        checkArgument(that != null);
        return Long.compare(getNextOffsetToLoad(), that.getNextOffsetToLoad());
    }

    public long getNextOffsetToLoad() {
        if (nextToLoad < 0) {
            return Long.MAX_VALUE;
        } else {
            return readingProgressRecorder.getCurrentFileOffset();
        }
    }

    public NettyConnectionId getNettyServiceWriterId() {
        return nettyServiceWriterId;
    }

    private class ReadingProgressRecorder {

        private final int subpartitionId;
        private int currentReadingBufferIndex;
        private int numBuffersReadable;
        private long currentFileOffset;

        public ReadingProgressRecorder(int subpartitionId) {
            this.subpartitionId = subpartitionId;
        }

        private int getRemainingBuffersInRegion(
                int bufferIndex, NettyConnectionId nettyServiceWriterId) {
            if (isInCachedRegion(bufferIndex)) {
                return numBuffersReadable;
            }
            Optional<RegionBufferIndexTracker.ReadableRegion> lookupResultOpt =
                    dataIndex.getReadableRegion(subpartitionId, bufferIndex, nettyServiceWriterId);
            if (!lookupResultOpt.isPresent()) {
                currentReadingBufferIndex = -1;
                numBuffersReadable = 0;
                currentFileOffset = -1L;
            } else {
                RegionBufferIndexTracker.ReadableRegion cachedRegion = lookupResultOpt.get();
                currentReadingBufferIndex = bufferIndex;
                numBuffersReadable = cachedRegion.numReadable;
                currentFileOffset = cachedRegion.offset;
            }
            return numBuffersReadable;
        }

        private long getCurrentFileOffset() {
            return currentReadingBufferIndex == -1 ? Long.MAX_VALUE : currentFileOffset;
        }

        private void advance(long bufferSize) {
            if (isInCachedRegion(currentReadingBufferIndex + 1)) {
                currentReadingBufferIndex++;
                numBuffersReadable--;
                currentFileOffset += bufferSize;
            }
        }

        // ------------------------------------------------------------------------
        //  Internal Methods
        // ------------------------------------------------------------------------

        private boolean isInCachedRegion(int bufferIndex) {
            return bufferIndex < currentReadingBufferIndex + numBuffersReadable
                    && bufferIndex >= currentReadingBufferIndex;
        }
    }
}
