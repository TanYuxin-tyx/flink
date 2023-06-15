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
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file.FileReaderId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file.PartitionFileReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.disk.RegionBufferIndexTracker;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/** The implementation of {@link ScheduledSubpartition}. */
public class ScheduledSubpartition implements Comparable<ScheduledSubpartition> {

    private final NettyConnectionId nettyServiceWriterId;

    private final int subpartitionId;

    private final int maxBufferReadAhead;

    private int nextToLoad = 0;

    private boolean isFailed;

    private final TieredStorageNettyService nettyService;

    private final NettyConnectionWriter nettyConnectionWriter;

    private final Map<Integer, Integer> firstBufferContextInSegment;

    private final FileReaderId fileReaderId = FileReaderId.newId();

    private final PartitionFileReader partitionFileReader;

    private final RegionBufferIndexTracker dataIndex;

    private final SubpartitionFileCache subpartitionFileCache;

    public ScheduledSubpartition(
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
        this.subpartitionFileCache = new SubpartitionFileCache(subpartitionId);
    }

    public void readBuffers(Queue<MemorySegment> buffers, BufferRecycler recycler)
            throws IOException {
        if (isFailed) {
            throw new IOException(
                    "The disk tier file reader of subpartition "
                            + subpartitionId
                            + " has already been failed.");
        }
        // If the number of loaded buffers achieves the limited value, skip this time.
        if (nettyConnectionWriter.numQueuedBuffers() >= maxBufferReadAhead) {
            return;
        }

        int numRemainingBuffer =
                subpartitionFileCache.getRemainingBuffersInRegion(nextToLoad, fileReaderId);
        // If there is no data in index, skip this time.
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
                                        subpartitionId, subpartitionFileCache.getCurrentFileOffset(), segment, recycler))
                        == null) {
                    buffers.add(segment);
                    break;
                }
            } catch (Throwable throwable) {
                buffers.add(segment);
                throw throwable;
            }
            int bufferLength = buffer.readableBytes() + BufferReaderWriterUtil.HEADER_LENGTH;
            subpartitionFileCache.advance(bufferLength);
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

    public FileReaderId getFileReaderId() {
        return fileReaderId;
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
    public int compareTo(ScheduledSubpartition that) {
        checkArgument(that != null);
        return Long.compare(getNextOffsetToLoad(), that.getNextOffsetToLoad());
    }

    public long getNextOffsetToLoad() {
        if (nextToLoad < 0) {
            return Long.MAX_VALUE;
        } else {
            return subpartitionFileCache.getCurrentFileOffset();
        }
    }

    private class SubpartitionFileCache {

        private final int subpartitionId;
        private int currentBufferIndex;
        private int numReadable;
        private long offset;

        public SubpartitionFileCache(int subpartitionId) {
            this.subpartitionId = subpartitionId;
        }

        private int getRemainingBuffersInRegion(
                int bufferIndex, FileReaderId nettyServiceWriterId) {
            updateCachedRegionIfNeeded(bufferIndex, nettyServiceWriterId);
            return numReadable;
        }

        private long getCurrentFileOffset() {
            return currentBufferIndex == -1 ? Long.MAX_VALUE : offset;
        }

        private void advance(long bufferSize) {
            if (isInCachedRegion(currentBufferIndex + 1)) {
                currentBufferIndex++;
                numReadable--;
                offset += bufferSize;
            }
        }

        // ------------------------------------------------------------------------
        //  Internal Methods
        // ------------------------------------------------------------------------

        private void updateCachedRegionIfNeeded(
                int bufferIndex, FileReaderId nettyServiceWriterId) {
            if (isInCachedRegion(bufferIndex)) {
                return;
            }
            Optional<RegionBufferIndexTracker.ReadableRegion> lookupResultOpt =
                    dataIndex.getReadableRegion(subpartitionId, bufferIndex, nettyServiceWriterId);
            if (!lookupResultOpt.isPresent()) {
                currentBufferIndex = -1;
                numReadable = 0;
                offset = -1L;
            } else {
                RegionBufferIndexTracker.ReadableRegion cachedRegion = lookupResultOpt.get();
                currentBufferIndex = bufferIndex;
                numReadable = cachedRegion.numReadable;
                offset = cachedRegion.offset;
            }
        }

        private boolean isInCachedRegion(int bufferIndex) {
            return bufferIndex < currentBufferIndex + numReadable
                    && bufferIndex >= currentBufferIndex;
        }
    }
}
