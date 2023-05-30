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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.netty2.NettyServiceWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.netty2.NettyServiceWriterId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.netty2.TieredStorageNettyService2;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.netty2.impl.TieredStorageNettyServiceImpl2;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.BufferContext;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.local.disk.RegionBufferIndexTracker;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Optional;
import java.util.Queue;
import java.util.function.Consumer;

import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.positionToNextBuffer;
import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.readFromByteChannel;
import static org.apache.flink.util.Preconditions.checkArgument;

/** The implementation of {@link ProducerMergePartitionSubpartitionReader}. */
public class ProducerMergePartitionSubpartitionReader
        implements Comparable<ProducerMergePartitionSubpartitionReader> {

    private final NettyServiceWriterId nettyServiceWriterId;

    private final ByteBuffer reusedHeaderBuffer;

    private final FileChannel dataFileChannel;

    private final RegionCache regionCache;

    private final RegionBufferIndexTracker dataIndex;

    private final int subpartitionId;

    private final int maxBufferReadAhead;

    private int nextToLoad = 0;

    private boolean isFailed;

    private final TieredStorageNettyService2 nettyService;

    private final NettyServiceWriter nettyServiceWriter;

    private ResultPartitionID id;

    public ProducerMergePartitionSubpartitionReader(
            ResultPartitionID id,
            int subpartitionId,
            int maxBufferReadAhead,
            ByteBuffer reusedHeaderBuffer,
            FileChannel dataFileChannel,
            RegionBufferIndexTracker dataIndex,
            Consumer<ProducerMergePartitionSubpartitionReader> readerReleaser,
            NettyServiceWriterId nettyServiceWriterId,
            TieredStorageNettyService2 nettyService) {
        this.id = id;
        this.subpartitionId = subpartitionId;
        this.nettyServiceWriterId = nettyServiceWriterId;
        this.dataFileChannel = dataFileChannel;
        this.reusedHeaderBuffer = reusedHeaderBuffer;
        this.maxBufferReadAhead = maxBufferReadAhead;
        this.dataIndex = dataIndex;
        this.nettyService = nettyService;
        this.nettyServiceWriter = nettyService.registerProducer(nettyServiceWriterId, () -> readerReleaser.accept(this));
        this.regionCache = new RegionCache();
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
        if (nettyServiceWriter.size() >= maxBufferReadAhead) {
            return;
        }
        int numRemainingBuffer =
                regionCache.getRemainingBuffersInRegion(nextToLoad, nettyServiceWriterId);
        // If there is no data in index, skip this time.
        if (numRemainingBuffer == 0) {
            return;
        }
        moveFileOffsetToBuffer();
        int numLoaded = 0;
        while (!buffers.isEmpty()
                && nettyServiceWriter.size() < maxBufferReadAhead
                && numRemainingBuffer-- > 0) {
            MemorySegment segment = buffers.poll();
            Buffer buffer;
            try {
                if ((buffer =
                                readFromByteChannel(
                                        dataFileChannel, reusedHeaderBuffer, segment, recycler))
                        == null) {
                    buffers.add(segment);
                    break;
                }
            } catch (Throwable throwable) {
                buffers.add(segment);
                throw throwable;
            }
            nettyServiceWriter.writeBuffer(new BufferContext(buffer, nextToLoad++, subpartitionId));
            regionCache.advance(buffer.readableBytes() + BufferReaderWriterUtil.HEADER_LENGTH);
            ++numLoaded;
        }
        if (nettyServiceWriter.size() <= numLoaded) {
            ((TieredStorageNettyServiceImpl2)nettyService).notifyResultSubpartitionViewSendBuffer(nettyServiceWriterId);
        }
    }

    public void fail(Throwable failureCause) {
        if (isFailed) {
            return;
        }
        isFailed = true;
        nettyServiceWriter.clear();
        nettyServiceWriter.writeBuffer(new BufferContext(failureCause));
        ((TieredStorageNettyServiceImpl2)nettyService).notifyResultSubpartitionViewSendBuffer(nettyServiceWriterId);
    }

    @Override
    public int compareTo(ProducerMergePartitionSubpartitionReader that) {
        checkArgument(that != null);
        return Long.compare(getNextOffsetToLoad(), that.getNextOffsetToLoad());
    }

    public long getNextOffsetToLoad() {
        if (nextToLoad < 0) {
            return Long.MAX_VALUE;
        } else {
            return regionCache.getNumSkipAndFileOffset().f1;
        }
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    private void moveFileOffsetToBuffer() throws IOException {
        Tuple2<Integer, Long> indexAndOffset = regionCache.getNumSkipAndFileOffset();
        dataFileChannel.position(indexAndOffset.f1);
        for (int i = 0; i < indexAndOffset.f0; ++i) {
            positionToNextBuffer(dataFileChannel, reusedHeaderBuffer);
        }
        regionCache.skipAll(dataFileChannel.position());
    }

    private class RegionCache {

        private int currentBufferIndex;
        private int numSkip;
        private int numReadable;
        private long offset;

        private int getRemainingBuffersInRegion(
                int bufferIndex, NettyServiceWriterId nettyServiceWriterId) {
            updateCachedRegionIfNeeded(bufferIndex, nettyServiceWriterId);
            return numReadable;
        }

        private Tuple2<Integer, Long> getNumSkipAndFileOffset() {
            return new Tuple2<>(numSkip, currentBufferIndex == -1 ? Long.MAX_VALUE : offset);
        }

        private void skipAll(long newOffset) {
            this.offset = newOffset;
            this.numSkip = 0;
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
                int bufferIndex, NettyServiceWriterId nettyServiceWriterId) {
            if (isInCachedRegion(bufferIndex)) {
                int numAdvance = bufferIndex - currentBufferIndex;
                numSkip += numAdvance;
                numReadable -= numAdvance;
                currentBufferIndex = bufferIndex;
                return;
            }

            Optional<RegionBufferIndexTracker.ReadableRegion> lookupResultOpt =
                    dataIndex.getReadableRegion(
                            subpartitionId, bufferIndex, nettyServiceWriterId);
            if (!lookupResultOpt.isPresent()) {
                currentBufferIndex = -1;
                numReadable = 0;
                numSkip = 0;
                offset = -1L;
            } else {
                RegionBufferIndexTracker.ReadableRegion cachedRegion = lookupResultOpt.get();
                currentBufferIndex = bufferIndex;
                numSkip = cachedRegion.numSkip;
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
