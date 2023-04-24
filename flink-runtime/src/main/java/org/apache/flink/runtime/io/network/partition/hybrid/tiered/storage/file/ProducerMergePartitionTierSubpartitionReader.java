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
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.BufferContext;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.local.disk.RegionBufferIndexTracker;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.service.NettyServiceViewId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.service.NettyBufferQueue;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.service.NettyBufferQueueImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.service.NettyServiceView;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Deque;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Consumer;

import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.positionToNextBuffer;
import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.readFromByteChannel;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The {@link ProducerMergePartitionTierSubpartitionReader} is responded to read data from
 * subpartition.
 */
public class ProducerMergePartitionTierSubpartitionReader
        implements Comparable<ProducerMergePartitionTierSubpartitionReader> {

    private final ByteBuffer headerBuf;

    private final int subpartitionId;

    private final NettyServiceViewId nettyServiceViewId;

    private final FileChannel dataFileChannel;

    private final NettyServiceView tierConsumerView;

    private final CachedRegionManager cachedRegionManager;

    private final Deque<BufferContext> loadedBuffers = new LinkedBlockingDeque<>();

    private final Consumer<ProducerMergePartitionTierSubpartitionReader> diskTierReaderReleaser;

    private final int maxBufferReadAhead;

    private int nextToLoad = 0;

    private volatile boolean isFailed;

    public ProducerMergePartitionTierSubpartitionReader(
            int subpartitionId,
            NettyServiceViewId nettyServiceViewId,
            FileChannel dataFileChannel,
            NettyServiceView tierConsumerView,
            RegionBufferIndexTracker dataIndex,
            int maxBufferReadAhead,
            Consumer<ProducerMergePartitionTierSubpartitionReader> diskTierReaderReleaser,
            ByteBuffer headerBuf) {
        this.subpartitionId = subpartitionId;
        this.nettyServiceViewId = nettyServiceViewId;
        this.dataFileChannel = dataFileChannel;
        this.tierConsumerView = tierConsumerView;
        this.headerBuf = headerBuf;
        this.maxBufferReadAhead = maxBufferReadAhead;
        this.cachedRegionManager = new CachedRegionManager(subpartitionId, dataIndex);
        this.diskTierReaderReleaser = diskTierReaderReleaser;
    }

    public NettyBufferQueue createNettyBufferQueue() {
        return new NettyBufferQueueImpl(loadedBuffers, () -> diskTierReaderReleaser.accept(this));
    }

    public synchronized void readBuffers(Queue<MemorySegment> buffers, BufferRecycler recycler)
            throws IOException {
        if (isFailed) {
            throw new IOException("subpartition reader has already failed.");
        }
        // If the number of loaded buffers achieves the limited value, skip this time.
        if (loadedBuffers.size() >= maxBufferReadAhead) {
            return;
        }
        int numRemainingBuffer =
                cachedRegionManager.getRemainingBuffersInRegion(nextToLoad, nettyServiceViewId);
        // If there is no data in index, skip this time.
        if (numRemainingBuffer == 0) {
            return;
        }
        moveFileOffsetToBuffer(nextToLoad);
        int numLoaded = 0;
        while (!buffers.isEmpty()
                && loadedBuffers.size() < maxBufferReadAhead
                && numRemainingBuffer-- > 0) {
            MemorySegment segment = buffers.poll();
            Buffer buffer;
            try {
                if ((buffer = readFromByteChannel(dataFileChannel, headerBuf, segment, recycler))
                        == null) {
                    buffers.add(segment);
                    break;
                }
            } catch (Throwable throwable) {
                buffers.add(segment);
                throw throwable;
            }
            loadedBuffers.add(new BufferContext(buffer, nextToLoad, subpartitionId));
            cachedRegionManager.advance(
                    buffer.readableBytes() + BufferReaderWriterUtil.HEADER_LENGTH);
            ++numLoaded;
            ++nextToLoad;
        }
        if (loadedBuffers.size() <= numLoaded) {
            tierConsumerView.notifyDataAvailable();
        }
    }

    public synchronized void fail(Throwable failureCause) {
        if (isFailed) {
            return;
        }
        isFailed = true;
        BufferContext bufferIndexOrError;
        // empty from tail, in-case subpartition view consumes concurrently and gets the wrong order
        while ((bufferIndexOrError = loadedBuffers.pollLast()) != null) {
            if (bufferIndexOrError.getBuffer() != null) {
                checkNotNull(bufferIndexOrError.getBuffer()).recycleBuffer();
            }
        }
        loadedBuffers.add(new BufferContext(null, null, failureCause));
        tierConsumerView.notifyDataAvailable();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ProducerMergePartitionTierSubpartitionReader that =
                (ProducerMergePartitionTierSubpartitionReader) o;
        return subpartitionId == that.subpartitionId
                && Objects.equals(nettyServiceViewId, that.nettyServiceViewId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subpartitionId, nettyServiceViewId);
    }

    public int compareTo(ProducerMergePartitionTierSubpartitionReader that) {
        checkArgument(that != null);
        return Long.compare(getNextOffsetToLoad(), that.getNextOffsetToLoad());
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    private void moveFileOffsetToBuffer(int bufferIndex) throws IOException {
        Tuple2<Integer, Long> indexAndOffset =
                cachedRegionManager.getNumSkipAndFileOffset(bufferIndex);
        dataFileChannel.position(indexAndOffset.f1);
        for (int i = 0; i < indexAndOffset.f0; ++i) {
            positionToNextBuffer(dataFileChannel, headerBuf);
        }
        cachedRegionManager.skipAll(dataFileChannel.position());
    }

    private long getNextOffsetToLoad() {
        if (nextToLoad < 0) {
            return Long.MAX_VALUE;
        } else {
            return cachedRegionManager.getFileOffset();
        }
    }

    private static class CachedRegionManager {
        private final int subpartitionId;
        private final RegionBufferIndexTracker dataIndex;

        private int currentBufferIndex;
        private int numSkip;
        private int numReadable;
        private long offset;

        private CachedRegionManager(int subpartitionId, RegionBufferIndexTracker dataIndex) {
            this.subpartitionId = subpartitionId;
            this.dataIndex = dataIndex;
        }
        /** Return Long.MAX_VALUE if region does not exist to giving the lowest priority. */
        private long getFileOffset() {
            return currentBufferIndex == -1 ? Long.MAX_VALUE : offset;
        }

        private int getRemainingBuffersInRegion(
                int bufferIndex, NettyServiceViewId nettyServiceViewId) {
            updateCachedRegionIfNeeded(bufferIndex, nettyServiceViewId);

            return numReadable;
        }

        private void skipAll(long newOffset) {
            this.offset = newOffset;
            this.numSkip = 0;
        }

        /**
         * Maps the given buffer index to the offset in file.
         *
         * @return a tuple of {@code <numSkip,offset>}. The offset of the given buffer index can be
         *     derived by starting from the {@code offset} and skipping {@code numSkip} buffers.
         */
        private Tuple2<Integer, Long> getNumSkipAndFileOffset(int bufferIndex) {
            checkState(numSkip >= 0, "num skip must be greater than or equal to 0");
            // Assumption: buffer index is always requested / updated increasingly
            checkState(currentBufferIndex <= bufferIndex);
            return new Tuple2<>(numSkip, offset);
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

        /** Points the cursors to the given buffer index, if possible. */
        private void updateCachedRegionIfNeeded(
                int bufferIndex, NettyServiceViewId nettyServiceViewId) {
            if (isInCachedRegion(bufferIndex)) {
                int numAdvance = bufferIndex - currentBufferIndex;
                numSkip += numAdvance;
                numReadable -= numAdvance;
                currentBufferIndex = bufferIndex;
                return;
            }

            Optional<RegionBufferIndexTracker.ReadableRegion> lookupResultOpt =
                    dataIndex.getReadableRegion(subpartitionId, bufferIndex, nettyServiceViewId);
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

    public static class Factory {

        public static final Factory INSTANCE = new Factory();

        private Factory() {}

        public ProducerMergePartitionTierSubpartitionReader createFileReader(
                int subpartitionId,
                NettyServiceViewId nettyServiceViewId,
                FileChannel dataFileChannel,
                NettyServiceView tierConsumerView,
                RegionBufferIndexTracker dataIndex,
                int maxBuffersReadAhead,
                Consumer<ProducerMergePartitionTierSubpartitionReader> fileReaderReleaser,
                ByteBuffer headerBuffer) {
            return new ProducerMergePartitionTierSubpartitionReader(
                    subpartitionId,
                    nettyServiceViewId,
                    dataFileChannel,
                    tierConsumerView,
                    dataIndex,
                    maxBuffersReadAhead,
                    fileReaderReleaser,
                    headerBuffer);
        }
    }
}
