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
import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyPayload;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyServiceImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file.PartitionFileIndex;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file.PartitionFileIndex.Region;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file.PartitionFileReader;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/** The implementation of {@link ScheduledSubpartitionReaderImpl}. */
public class ScheduledSubpartitionReaderImpl implements ScheduledSubpartitionReader {

    private final NettyConnectionId nettyServiceWriterId;

    private final int subpartitionId;

    private final int maxBufferReadAhead;

    private final TieredStorageNettyService nettyService;

    private final NettyConnectionWriter nettyConnectionWriter;

    private final Map<Integer, Integer> segmentIdRecorder;

    private final PartitionFileReader partitionFileReader;

    private final PartitionFileIndex dataIndex;

    private final SubpartitionReaderProgress subpartitionReaderProgress;

    private int nextToLoad = 0;

    private boolean isFailed;

    public ScheduledSubpartitionReaderImpl(
            int subpartitionId,
            int maxBufferReadAhead,
            NettyConnectionWriter nettyConnectionWriter,
            TieredStorageNettyService nettyService,
            Map<Integer, Integer> segmentIdRecorder,
            PartitionFileReader partitionFileReader,
            PartitionFileIndex dataIndex) {
        this.subpartitionId = subpartitionId;
        this.nettyServiceWriterId = nettyConnectionWriter.getNettyConnectionId();
        this.maxBufferReadAhead = maxBufferReadAhead;
        this.nettyService = nettyService;
        this.nettyConnectionWriter = nettyConnectionWriter;
        this.segmentIdRecorder = segmentIdRecorder;
        this.partitionFileReader = partitionFileReader;
        this.dataIndex = dataIndex;
        this.subpartitionReaderProgress = new SubpartitionReaderProgress(subpartitionId);
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
        int numRemainingBuffer =
                subpartitionReaderProgress.getReadableBufferNumber(nettyServiceWriterId);
        if (numRemainingBuffer == 0) {
            return;
        }
        int numLoaded = 0;
        while (!buffers.isEmpty()
                && nettyConnectionWriter.numQueuedBuffers() < maxBufferReadAhead
                && numRemainingBuffer-- > 0) {
            MemorySegment memorySegment = buffers.poll();
            Buffer buffer;
            try {
                if ((buffer =
                                partitionFileReader.readBuffer(
                                        subpartitionId,
                                        -1,
                                        subpartitionReaderProgress.getCurrentFileOffset(),
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
            subpartitionReaderProgress.advance(
                    buffer.readableBytes() + BufferReaderWriterUtil.HEADER_LENGTH);
            NettyPayload nettyPayload =
                    NettyPayload.newBuffer(buffer, nextToLoad++, subpartitionId);
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
        return subpartitionReaderProgress.getCurrentFileOffset();
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

    /**
     * {@link SubpartitionReaderProgress} is used to record the necessary information of reading
     * progress of a subpartition reader, which includes the id of subpartition, current file
     * offset, and the number of available buffers in the subpartition.
     */
    private class SubpartitionReaderProgress {

        private final int subpartitionId;

        private long currentFileOffset = Long.MAX_VALUE;

        private int numBuffersReadable;

        public SubpartitionReaderProgress(int subpartitionId) {
            this.subpartitionId = subpartitionId;
        }

        /**
         * Get the number of readable buffers for a {@link NettyConnectionWriter}.
         *
         * @param nettyServiceWriterId the id of netty service writer.
         * @return the number of readable buffers.
         */
        private int getReadableBufferNumber(NettyConnectionId nettyServiceWriterId) {
            if (numBuffersReadable == 0) {
                Optional<Region> region =
                        dataIndex.getNextRegion(subpartitionId, nettyServiceWriterId);
                if (region.isPresent()) {
                    numBuffersReadable = region.get().getNumBuffers();
                    currentFileOffset = region.get().getRegionFileOffset();
                }
            }
            return numBuffersReadable;
        }

        /**
         * Get the current file offset.
         *
         * @return the file offset.
         */
        private long getCurrentFileOffset() {
            return currentFileOffset;
        }

        /**
         * Update the progress.
         *
         * @param bufferSize is the size of buffer.
         */
        private void advance(long bufferSize) {
            numBuffersReadable--;
            currentFileOffset += bufferSize;
            checkState(numBuffersReadable >= 0);
        }
    }
}
