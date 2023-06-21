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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.readFromByteChannel;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The implementation of {@link PartitionFileReader} with producer-merge mode. In this mode, the
 * shuffle data is written in the producer side, the consumer side need to read multiple producers
 * to get its partition data.
 *
 * <p>Note that one partition file may contain the data of multiple subpartitions.
 */
public class ProducerMergePartitionFileReader implements PartitionFileReader {

    private final Map<TieredStorageSubpartitionId, List<SubpartitionFileReadProgress>>
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
        List<SubpartitionFileReadProgress> progresses =
                subpartitionReadCache.computeIfAbsent(subpartitionId, ignore -> new ArrayList<>());

        SubpartitionFileReadProgress matchedReadProgress =
                getMatchedReadProgress(progresses, bufferIndex);
        if (matchedReadProgress == null) {
            checkState(bufferIndex == 0);
            matchedReadProgress = new SubpartitionFileReadProgress(subpartitionId);
            progresses.add(matchedReadProgress);
        }

        if (!matchedReadProgress.hasBuffer()) {
            return null;
        }
        long fileOffSet = matchedReadProgress.getCurrentFileOffset();
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
        matchedReadProgress.advance(
                checkNotNull(buffer).readableBytes() + BufferReaderWriterUtil.HEADER_LENGTH);
        return buffer;
    }

    @Override
    public long getFileOffset(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId,
            int bufferIndex) {
        List<SubpartitionFileReadProgress> progresses =
                subpartitionReadCache.computeIfAbsent(subpartitionId, ignore -> new ArrayList<>());
        for (SubpartitionFileReadProgress progress : progresses) {
            if (progress.getCurrentBufferIndex() == bufferIndex) {
                return progress.currentFileOffset;
            }
        }
        return 0;
    }

    @Override
    public void release() {
        fileChannel = null;
        IOUtils.deleteFileQuietly(dataFilePath);
    }

    private SubpartitionFileReadProgress getMatchedReadProgress(
            List<SubpartitionFileReadProgress> progresses, int bufferIndex) {
        for (SubpartitionFileReadProgress progress : progresses) {
            if (progress.getCurrentBufferIndex() == bufferIndex) {
                return progress;
            }
        }
        return null;
    }

    /**
     * {@link SubpartitionFileReadProgress} is used to record the necessary information of reading
     * progress of a subpartition reader, which includes the id of subpartition, current file
     * offset, and the number of available buffers in the subpartition.
     */
    private class SubpartitionFileReadProgress {

        private final TieredStorageSubpartitionId subpartitionId;

        private long currentFileOffset = Long.MAX_VALUE;

        private int regionId = 0;

        private int numBuffersReadable;

        private int bufferIndex = 0;

        public SubpartitionFileReadProgress(TieredStorageSubpartitionId subpartitionId) {
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
         * Update the progress.
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
