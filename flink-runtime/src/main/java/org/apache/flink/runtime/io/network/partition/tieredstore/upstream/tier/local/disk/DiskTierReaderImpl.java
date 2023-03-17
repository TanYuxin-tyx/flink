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

package org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.BufferIndexOrError;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierReaderView;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierReaderViewId;

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
 * Default implementation of {@link DiskTierReader}.
 *
 * <p>Note: This class is not thread safe.
 */
public class DiskTierReaderImpl implements DiskTierReader {

    private final ByteBuffer headerBuf;

    private final int subpartitionId;

    private final TierReaderViewId tierReaderViewId;

    private final FileChannel dataFileChannel;

    private final TierReaderView tierReaderView;

    private final CachedRegionManager cachedRegionManager;

    private final BufferIndexManager bufferIndexManager;

    private final Deque<BufferIndexOrError> loadedBuffers = new LinkedBlockingDeque<>();

    private final Consumer<DiskTierReader> diskTierReaderReleaser;

    private volatile boolean isFailed;

    public DiskTierReaderImpl(
            int subpartitionId,
            TierReaderViewId tierReaderViewId,
            FileChannel dataFileChannel,
            TierReaderView tierReaderView,
            RegionBufferIndexTracker dataIndex,
            int maxBufferReadAhead,
            Consumer<DiskTierReader> diskTierReaderReleaser,
            ByteBuffer headerBuf) {
        this.subpartitionId = subpartitionId;
        this.tierReaderViewId = tierReaderViewId;
        this.dataFileChannel = dataFileChannel;
        this.tierReaderView = tierReaderView;
        this.headerBuf = headerBuf;
        this.bufferIndexManager = new BufferIndexManager(maxBufferReadAhead);
        this.cachedRegionManager = new CachedRegionManager(subpartitionId, dataIndex);
        this.diskTierReaderReleaser = diskTierReaderReleaser;
    }

    @Override
    public synchronized void readBuffers(Queue<MemorySegment> buffers, BufferRecycler recycler)
            throws IOException {
        if (isFailed) {
            throw new IOException("subpartition reader has already failed.");
        }
        // If the number of loaded buffers achieves the limited value, skip this time.
        int firstBufferToLoad = bufferIndexManager.getNextToLoad();
        if (firstBufferToLoad < 0) {
            return;
        }
        int numRemainingBuffer =
                cachedRegionManager.getRemainingBuffersInRegion(
                        firstBufferToLoad, tierReaderViewId);
        // If there is no data in index, skip this time.
        if (numRemainingBuffer == 0) {
            return;
        }
        moveFileOffsetToBuffer(firstBufferToLoad);

        int indexToLoad;
        int numLoaded = 0;
        while (!buffers.isEmpty()
                && (indexToLoad = bufferIndexManager.getNextToLoad()) >= 0
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
            loadedBuffers.add(BufferIndexOrError.newBuffer(buffer, indexToLoad));
            bufferIndexManager.updateLastLoaded(indexToLoad);
            cachedRegionManager.advance(
                    buffer.readableBytes() + BufferReaderWriterUtil.HEADER_LENGTH);
            ++numLoaded;
        }
        if (loadedBuffers.size() <= numLoaded) {
            tierReaderView.notifyDataAvailable();
        }
    }

    @Override
    public Optional<ResultSubpartition.BufferAndBacklog> consumeBuffer(int nextBufferToConsume)
            throws Throwable {
        if (!checkAndGetFirstBufferIndexOrError(nextBufferToConsume).isPresent()) {
            return Optional.empty();
        }

        // already ensure that peek element is not null and not throwable.
        BufferIndexOrError current = checkNotNull(loadedBuffers.poll());

        BufferIndexOrError next = loadedBuffers.peek();

        Buffer.DataType nextDataType = next == null ? Buffer.DataType.NONE : next.getDataType();
        int backlog = loadedBuffers.size();
        int bufferIndex = current.getIndex();
        Buffer buffer =
                current.getBuffer()
                        .orElseThrow(
                                () ->
                                        new NullPointerException(
                                                "Get a non-throwable and non-buffer bufferIndexOrError, which is not allowed"));
        return Optional.of(
                ResultSubpartition.BufferAndBacklog.fromBufferAndLookahead(
                        buffer,
                        nextDataType,
                        backlog,
                        bufferIndex));
    }

    @Override
    public synchronized void fail(Throwable failureCause) {
        if (isFailed) {
            return;
        }
        isFailed = true;
        BufferIndexOrError bufferIndexOrError;
        // empty from tail, in-case subpartition view consumes concurrently and gets the wrong order
        while ((bufferIndexOrError = loadedBuffers.pollLast()) != null) {
            if (bufferIndexOrError.getBuffer().isPresent()) {
                checkNotNull(bufferIndexOrError.getBuffer().get()).recycleBuffer();
            }
        }
        loadedBuffers.add(BufferIndexOrError.newError(failureCause));
        tierReaderView.notifyDataAvailable();
    }

    @Override
    public void release() {
        BufferIndexOrError bufferIndexOrError;
        while ((bufferIndexOrError = loadedBuffers.pollLast()) != null) {
            if (bufferIndexOrError.getBuffer().isPresent()) {
                checkNotNull(bufferIndexOrError.getBuffer().get()).recycleBuffer();
            }
        }
        diskTierReaderReleaser.accept(this);
    }

    @Override
    public void prepareForScheduling() {
        // Access the consuming offset with lock, to prevent loading any buffer released from the
        // memory data manager that is already consumed.
        int consumingOffset = tierReaderView.getConsumingOffset(true);
        bufferIndexManager.updateLastConsumed(consumingOffset);
        cachedRegionManager.updateConsumingOffset(consumingOffset);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DiskTierReaderImpl that = (DiskTierReaderImpl) o;
        return subpartitionId == that.subpartitionId
                && Objects.equals(tierReaderViewId, that.tierReaderViewId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subpartitionId, tierReaderViewId);
    }

    @Override
    public int compareTo(DiskTierReader that) {
        checkArgument(that instanceof DiskTierReaderImpl);
        return Long.compare(
                getNextOffsetToLoad(), ((DiskTierReaderImpl) that).getNextOffsetToLoad());
    }

    @Override
    public int getBacklog() {
        return loadedBuffers.size();
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    private Optional<BufferIndexOrError> checkAndGetFirstBufferIndexOrError(int expectedBufferIndex)
            throws Throwable {
        if (loadedBuffers.isEmpty()) {
            return Optional.empty();
        }

        BufferIndexOrError peek = loadedBuffers.peek();
        if (peek.getThrowable().isPresent()) {
            throw peek.getThrowable().get();
        }
        checkState(peek.getIndex() == expectedBufferIndex);
        return Optional.of(peek);
    }

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
        int bufferIndex = bufferIndexManager.getNextToLoad();
        if (bufferIndex < 0) {
            return Long.MAX_VALUE;
        } else {
            return cachedRegionManager.getFileOffset();
        }
    }

    static class BufferIndexManager {

        private final int maxBuffersReadAhead;

        /** Index of the last buffer that has ever been loaded from file. */
        private int lastLoaded = -1;
        /** Index of the last buffer that has been consumed by downstream, to the best knowledge. */
        private int lastConsumed = -1;

        BufferIndexManager(int maxBuffersReadAhead) {
            this.maxBuffersReadAhead = maxBuffersReadAhead;
        }

        private void updateLastLoaded(int lastLoaded) {
            checkState(this.lastLoaded <= lastLoaded);
            this.lastLoaded = lastLoaded;
        }

        private void updateLastConsumed(int lastConsumed) {
            this.lastConsumed = lastConsumed;
        }

        /** Returns a negative value if shouldn't load. */
        private int getNextToLoad() {
            checkState(lastLoaded >= lastConsumed);
            int nextToLoad = lastLoaded + 1;
            int maxToLoad = lastConsumed + maxBuffersReadAhead;
            return nextToLoad <= maxToLoad ? nextToLoad : -1;
        }
    }

    private static class CachedRegionManager {
        private final int subpartitionId;
        private final RegionBufferIndexTracker dataIndex;

        private int consumingOffset = -1;

        private int currentBufferIndex;
        private int numSkip;
        private int numReadable;
        private long offset;

        private CachedRegionManager(int subpartitionId, RegionBufferIndexTracker dataIndex) {
            this.subpartitionId = subpartitionId;
            this.dataIndex = dataIndex;
        }

        public void updateConsumingOffset(int consumingOffset) {
            this.consumingOffset = consumingOffset;
        }

        /** Return Long.MAX_VALUE if region does not exist to giving the lowest priority. */
        private long getFileOffset() {
            return currentBufferIndex == -1 ? Long.MAX_VALUE : offset;
        }

        private int getRemainingBuffersInRegion(
                int bufferIndex, TierReaderViewId tierReaderViewId) {
            updateCachedRegionIfNeeded(bufferIndex, tierReaderViewId);

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
                int bufferIndex, TierReaderViewId tierReaderViewId) {
            if (isInCachedRegion(bufferIndex)) {
                int numAdvance = bufferIndex - currentBufferIndex;
                numSkip += numAdvance;
                numReadable -= numAdvance;
                currentBufferIndex = bufferIndex;
                return;
            }

            Optional<RegionBufferIndexTracker.ReadableRegion> lookupResultOpt =
                    dataIndex.getReadableRegion(
                            subpartitionId, bufferIndex, consumingOffset, tierReaderViewId);
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

    /** Factory of {@link DiskTierReader}. */
    public static class Factory implements DiskTierReader.Factory {
        public static final Factory INSTANCE = new Factory();

        private Factory() {}

        @Override
        public DiskTierReader createFileReader(
                int subpartitionId,
                TierReaderViewId tierReaderViewId,
                FileChannel dataFileChannel,
                TierReaderView tierReaderView,
                RegionBufferIndexTracker dataIndex,
                int maxBuffersReadAhead,
                Consumer<DiskTierReader> fileReaderReleaser,
                ByteBuffer headerBuffer) {
            return new DiskTierReaderImpl(
                    subpartitionId,
                    tierReaderViewId,
                    dataFileChannel,
                    tierReaderView,
                    dataIndex,
                    maxBuffersReadAhead,
                    fileReaderReleaser,
                    headerBuffer);
        }
    }
}
