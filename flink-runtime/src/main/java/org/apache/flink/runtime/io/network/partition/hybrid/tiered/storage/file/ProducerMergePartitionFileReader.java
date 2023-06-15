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
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.disk.RegionBufferIndexTracker;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.positionToNextBuffer;
import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.readFromByteChannel;

public class ProducerMergePartitionFileReader implements PartitionFileReader {

    private final ByteBuffer reusedHeaderBuffer = BufferReaderWriterUtil.allocatedHeaderBuffer();

    private final RegionBufferIndexTracker dataIndex;

    private final Map<FileReaderId, SubpartitionFileCache> allFileCaches =
            new ConcurrentHashMap<>();

    private final Path dataFilePath;

    private FileChannel fileChannel;

    public ProducerMergePartitionFileReader(Path dataFilePath, RegionBufferIndexTracker dataIndex) {
        this.dataFilePath = dataFilePath;
        this.dataIndex = dataIndex;
    }

    @Override
    public Buffer readBuffer(
            int subpartitionId, FileReaderId id, MemorySegment segment, BufferRecycler recycler) {
        if (fileChannel == null) {
            return null;
        }
        SubpartitionFileCache subpartitionFileCache =
                allFileCaches.computeIfAbsent(
                        id, ignore -> new SubpartitionFileCache(subpartitionId));
        Buffer buffer = null;
        try {
            buffer = readFromByteChannel(fileChannel, reusedHeaderBuffer, segment, recycler);
        } catch (IOException e) {
            ExceptionUtils.rethrow(e, "Failed to read buffer.");
        }
        if (buffer != null) {
            subpartitionFileCache.advance(
                    buffer.readableBytes() + BufferReaderWriterUtil.HEADER_LENGTH);
        }
        return buffer;
    }

    @Override
    public long getFileOffset(int subpartitionId, FileReaderId id) {
        SubpartitionFileCache subpartitionFileCache =
                allFileCaches.computeIfAbsent(
                        id, ignore -> new SubpartitionFileCache(subpartitionId));
        return subpartitionFileCache.getNumSkipAndFileOffset().f1;
    }

    @Override
    public int getReadableBuffers(int subpartitionId, int currentBufferIndex, FileReaderId id) {
        if (fileChannel == null) {
            try {
                fileChannel = FileChannel.open(dataFilePath, StandardOpenOption.READ);
            } catch (IOException e) {
                throw new RuntimeException("Failed to open a file channel.", e);
            }
        }
        SubpartitionFileCache subpartitionFileCache =
                allFileCaches.computeIfAbsent(
                        id, ignore -> new SubpartitionFileCache(subpartitionId));
        int remainingBuffersInRegion =
                subpartitionFileCache.getRemainingBuffersInRegion(currentBufferIndex, id);
        if (remainingBuffersInRegion > 0) {
            moveFileOffsetToBuffer(subpartitionFileCache);
        }
        return remainingBuffersInRegion;
    }

    private void moveFileOffsetToBuffer(SubpartitionFileCache subpartitionFileCache) {
        if (fileChannel == null) {
            return;
        }
        Tuple2<Integer, Long> indexAndOffset = subpartitionFileCache.getNumSkipAndFileOffset();
        try {
            fileChannel.position(indexAndOffset.f1);
            for (int i = 0; i < indexAndOffset.f0; ++i) {
                positionToNextBuffer(fileChannel, reusedHeaderBuffer);
            }
            subpartitionFileCache.skipAll(fileChannel.position());
        } catch (IOException e) {
            ExceptionUtils.rethrow(e, "Failed to move file offset to buffer.");
        }
    }

    @Override
    public void release() {
        fileChannel = null;
        IOUtils.deleteFileQuietly(dataFilePath);
    }

    private class SubpartitionFileCache {

        private final int subpartitionId;
        private int currentBufferIndex;
        private int numSkip;
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
                int bufferIndex, FileReaderId nettyServiceWriterId) {
            if (isInCachedRegion(bufferIndex)) {
                int numAdvance = bufferIndex - currentBufferIndex;
                numSkip += numAdvance;
                numReadable -= numAdvance;
                currentBufferIndex = bufferIndex;
                return;
            }

            Optional<RegionBufferIndexTracker.ReadableRegion> lookupResultOpt =
                    dataIndex.getReadableRegion(subpartitionId, bufferIndex, nettyServiceWriterId);
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
