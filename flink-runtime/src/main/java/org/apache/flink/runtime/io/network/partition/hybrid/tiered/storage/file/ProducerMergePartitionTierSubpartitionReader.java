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
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.service.NettyBufferQueue;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.service.NettyBufferQueueImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.service.NettyServiceView;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.service.NettyServiceViewId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.local.disk.RegionBufferIndexTracker;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Deque;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Consumer;

import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.positionToNextBuffer;
import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.readFromByteChannel;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The {@link ProducerMergePartitionTierSubpartitionReader} is responded to load buffers of a
 * subpartition from disk.
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
            throw new IOException(
                    "The disk reader of subpartition "
                            + subpartitionId
                            + " has already been failed.");
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
        moveFileOffsetToBuffer();
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

    public void fail(Throwable failureCause) {
        if (isFailed) {
            return;
        }
        isFailed = true;
        BufferContext loadedBuffer;
        while ((loadedBuffer = loadedBuffers.pollLast()) != null) {
            if (loadedBuffer.getBuffer() != null) {
                checkNotNull(loadedBuffer.getBuffer()).recycleBuffer();
            }
        }
        loadedBuffers.add(new BufferContext(null, null, failureCause));
        tierConsumerView.notifyDataAvailable();
    }

    public int compareTo(ProducerMergePartitionTierSubpartitionReader that) {
        checkArgument(that != null);
        return Long.compare(getNextOffsetToLoad(), that.getNextOffsetToLoad());
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    private void moveFileOffsetToBuffer() throws IOException {
        Tuple2<Integer, Long> indexAndOffset =
                cachedRegionManager.getNumSkipAndFileOffset();
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
}
