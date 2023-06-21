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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.file;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageIdMappingUtils;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IOUtils;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;

import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.readFromByteChannel;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The implementation of {@link PartitionFileReader} with producer-merge mode. In this mode, the
 * shuffle data is written in the producer side with a single file.
 */
public class ProducerMergePartitionFileReader implements PartitionFileReader {

    private final Map<TieredStorageSubpartitionId, Map<Integer, Queue<SubpartitionReadCache>>>
            subpartitionReadCache;

    private final ByteBuffer reusedHeaderBuffer = BufferReaderWriterUtil.allocatedHeaderBuffer();

    private final Path dataFilePath;

    @Nullable private FileChannel fileChannel;

    private final PartitionFileIndex dataIndex;

    ProducerMergePartitionFileReader(Path dataFilePath, PartitionFileIndex dataIndex) {
        this.dataFilePath = dataFilePath;
        this.dataIndex = dataIndex;
        this.subpartitionReadCache = new HashMap<>();
    }

    @Override
    public Buffer readBuffer(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId,
            int bufferIndex,
            MemorySegment segment,
            BufferRecycler recycler)
            throws IOException {
        Map<Integer, Queue<SubpartitionReadCache>> progresses =
                subpartitionReadCache.computeIfAbsent(subpartitionId, ignore -> new HashMap<>());
        Queue<SubpartitionReadCache> subpartitionProgresses =
                progresses.computeIfAbsent(bufferIndex, ignore -> new LinkedList<>());
        SubpartitionReadCache currentProgress =
                subpartitionProgresses.isEmpty() ? null : subpartitionProgresses.peek();
        if (currentProgress == null) {
            checkState(bufferIndex == 0);
            currentProgress = new SubpartitionReadCache(subpartitionId);
            subpartitionProgresses.add(currentProgress);
            progresses.put(0, subpartitionProgresses);
        }

        if (!currentProgress.hasBuffer()) {
            return null;
        }
        long fileOffSet = currentProgress.getCurrentFileOffset();
        if (fileChannel == null) {
            try {
                fileChannel = FileChannel.open(dataFilePath, StandardOpenOption.READ);
            } catch (FileNotFoundException e) {
                throw new PartitionNotFoundException(
                        TieredStorageIdMappingUtils.convertId(partitionId));
            }
        }
        try {
            fileChannel.position(fileOffSet);
        } catch (IOException e) {
            ExceptionUtils.rethrow(e, "Failed to position file offset to buffer.");
        }
        Buffer buffer = null;
        try {
            buffer = readFromByteChannel(fileChannel, reusedHeaderBuffer, segment, recycler);
        } catch (IOException e) {
            ExceptionUtils.rethrow(e, "Failed to read buffer.");
        }
        currentProgress.advance(
                checkNotNull(buffer).readableBytes() + BufferReaderWriterUtil.HEADER_LENGTH);
        subpartitionProgresses.poll();
        if (subpartitionProgresses.isEmpty()) {
            progresses.remove(bufferIndex);
        }
        progresses
                .computeIfAbsent(
                        currentProgress.getCurrentBufferIndex(), ignore -> new LinkedList<>())
                .add(currentProgress);
        return buffer;
    }

    @Override
    public long getPriority(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId,
            int bufferIndex) {
        Queue<SubpartitionReadCache> progress =
                subpartitionReadCache
                        .computeIfAbsent(subpartitionId, ignore -> new HashMap<>())
                        .computeIfAbsent(bufferIndex, ignore -> new LinkedList<>());
        return progress.isEmpty() ? 0 : progress.peek().getCurrentBufferIndex();
    }

    @Override
    public void release() {
        fileChannel = null;
        IOUtils.deleteFileQuietly(dataFilePath);
    }

    /**
     * {@link SubpartitionReadCache} is the cache to record the reading progress for a consumer of a
     * subpartition.
     */
    private class SubpartitionReadCache {

        private final TieredStorageSubpartitionId subpartitionId;

        private long currentFileOffset = Long.MAX_VALUE;

        private int regionId = 0;

        private int numBuffersReadable;

        private int bufferIndex = 0;

        public SubpartitionReadCache(TieredStorageSubpartitionId subpartitionId) {
            this.subpartitionId = subpartitionId;
        }

        public int getCurrentBufferIndex() {
            return bufferIndex;
        }

        private boolean hasBuffer() {
            if (numBuffersReadable == 0) {
                Optional<PartitionFileIndex.Region> region =
                        dataIndex.getRegion(subpartitionId.getSubpartitionId(), regionId);
                if (region.isPresent()) {
                    regionId++;
                    numBuffersReadable = region.get().getNumBuffers();
                    currentFileOffset = region.get().getRegionFileOffset();
                }
            }
            return numBuffersReadable != 0;
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
         * Update the cache.
         *
         * @param bufferSize is the size of buffer.
         */
        private void advance(long bufferSize) {
            bufferIndex++;
            numBuffersReadable--;
            currentFileOffset += bufferSize;
            checkState(numBuffersReadable >= 0);
        }
    }
}
