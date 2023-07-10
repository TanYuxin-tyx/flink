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

import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IOUtils;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * The {@link ProducerMergedPartitionFileIndex} is used by {@link ProducerMergedPartitionFileWriter}
 * and {@link ProducerMergedPartitionFileReader}, to maintain the offset of each buffer in the
 * physical file.
 *
 * <p>For efficiency, buffers from the same subpartition that are both logically (i.e. index in the
 * subpartition) and physically (i.e. offset in the file) consecutive are combined into a {@link
 * Region}.
 *
 * <pre>For example, the following buffers (indicated by subpartitionId-bufferIndex):
 *   1-1, 1-2, 1-3, 2-1, 2-2, 2-5, 1-4, 1-5, 2-6
 * will be combined into 5 regions (separated by '|'):
 *   1-1, 1-2, 1-3 | 2-1, 2-2 | 2-5 | 1-4, 1-5 | 2-6
 * </pre>
 */
public class ProducerMergedPartitionFileIndex {

    private final Path indexFilePath;

    /**
     * The regions belonging to each subpartitions.
     *
     * <p>Note that the field can be accessed by the writing and reading IO thread, so the lock is
     * to ensure the thread safety.
     */
    @GuardedBy("lock")
    private final ProducerMergedPartitionFileIndexCache indexCache;

    private final Object lock = new Object();

    public ProducerMergedPartitionFileIndex(
            int numSubpartitions,
            Path indexFilePath,
            int spilledIndexSegmentSize,
            long numRetainedInMemoryRegionsMax) {
        this.indexFilePath = indexFilePath;
        this.indexCache =
                new ProducerMergedPartitionFileIndexCache(
                        numSubpartitions,
                        indexFilePath,
                        numRetainedInMemoryRegionsMax,
                        new ProducerMergedPartitionFileIndexFlushManager.Factory(
                                spilledIndexSegmentSize, numRetainedInMemoryRegionsMax));
    }

    /**
     * Add buffers to the index.
     *
     * @param buffers to be added. Note, the provided buffers are required to be physically
     *     consecutive and in the same order as in the file.
     */
    void addBuffers(List<FlushedBuffer> buffers) {
        if (buffers.isEmpty()) {
            return;
        }

        Map<Integer, List<Region>> convertedRegions = convertToRegions(buffers);
        synchronized (lock) {
            convertedRegions.forEach(indexCache::put);
        }
    }

    /**
     * Get the subpartition's {@link Region} containing the specific buffer index.
     *
     * @param subpartitionId the subpartition id
     * @param bufferIndex the buffer index
     * @return the region containing the buffer index, or return emtpy if the region is not found.
     */
    Optional<Region> getRegion(TieredStorageSubpartitionId subpartitionId, int bufferIndex) {
        synchronized (lock) {
            return indexCache.get(subpartitionId.getSubpartitionId(), bufferIndex);
        }
    }

    void release() {
        synchronized (lock) {
            try {
                indexCache.close();
                IOUtils.deleteFileQuietly(indexFilePath);
            } catch (IOException e) {
                ExceptionUtils.rethrow(e);
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    private static Map<Integer, List<Region>> convertToRegions(List<FlushedBuffer> buffers) {
        Map<Integer, List<Region>> subpartitionRegionMap = new HashMap<>();
        Iterator<FlushedBuffer> iterator = buffers.iterator();
        FlushedBuffer firstBufferInRegion = iterator.next();
        FlushedBuffer lastBufferInRegion = firstBufferInRegion;

        while (iterator.hasNext()) {
            FlushedBuffer currentBuffer = iterator.next();
            if (currentBuffer.getSubpartitionId() != firstBufferInRegion.getSubpartitionId()
                    || currentBuffer.getBufferIndex() != lastBufferInRegion.getBufferIndex() + 1) {
                // The current buffer belongs to a new region, add the current region to the map
                addRegionToMap(firstBufferInRegion, lastBufferInRegion, subpartitionRegionMap);
                firstBufferInRegion = currentBuffer;
            }
            lastBufferInRegion = currentBuffer;
        }

        // Add the last region to the map
        addRegionToMap(firstBufferInRegion, lastBufferInRegion, subpartitionRegionMap);
        return subpartitionRegionMap;
    }

    private static void addRegionToMap(
            FlushedBuffer firstBufferInRegion,
            FlushedBuffer lastBufferInRegion,
            Map<Integer, List<Region>> subpartitionRegionMap) {
        checkArgument(
                firstBufferInRegion.getSubpartitionId() == lastBufferInRegion.getSubpartitionId());
        checkArgument(firstBufferInRegion.getBufferIndex() <= lastBufferInRegion.getBufferIndex());

        subpartitionRegionMap
                .computeIfAbsent(firstBufferInRegion.getSubpartitionId(), ArrayList::new)
                .add(
                        new Region(
                                firstBufferInRegion.getBufferIndex(),
                                firstBufferInRegion.getFileOffset(),
                                lastBufferInRegion.getBufferIndex()
                                        - firstBufferInRegion.getBufferIndex()
                                        + 1));
    }

    /**
     * Allocate a buffer with specific size and configure it to native order.
     *
     * @param bufferSize the size of buffer to allocate.
     * @return a native order buffer with expected size.
     */
    public static ByteBuffer allocateAndConfigureBuffer(int bufferSize) {
        ByteBuffer buffer = ByteBuffer.allocateDirect(bufferSize);
        buffer.order(ByteOrder.nativeOrder());
        return buffer;
    }

    /**
     * Write {@link Region} to {@link FileChannel}.
     *
     * @param channel the file's channel to write.
     * @param regionBuffer the buffer to write {@link Region}'s header.
     * @param region the region to be written to channel.
     */
    public static void writeRegionToFile(
            FileChannel channel, ByteBuffer regionBuffer, Region region) throws IOException {
        // write header buffer.
        regionBuffer.clear();
        regionBuffer.putInt(region.getFirstBufferIndex());
        regionBuffer.putInt(region.getNumBuffers());
        regionBuffer.putLong(region.getRegionFileOffset());
        regionBuffer.flip();
        BufferReaderWriterUtil.writeBuffers(channel, regionBuffer.capacity(), regionBuffer);
    }

    /**
     * Read {@link Region} from {@link FileChannel}.
     *
     * @param channel the channel to read.
     * @param regionBuffer the buffer to read {@link Region}'s header.
     * @param position position to start read.
     * @return the {@link Region} that read from this channel.
     */
    public static Region readRegionFromFile(
            FileChannel channel, ByteBuffer regionBuffer, long position) throws IOException {
        regionBuffer.clear();
        BufferReaderWriterUtil.readByteBufferFully(channel, regionBuffer, position);
        regionBuffer.flip();
        int firstBufferIndex = regionBuffer.getInt();
        int numBuffers = regionBuffer.getInt();
        long firstBufferOffset = regionBuffer.getLong();
        return new Region(firstBufferIndex, firstBufferOffset, numBuffers);
    }

    // ------------------------------------------------------------------------
    //  Internal Classes
    // ------------------------------------------------------------------------

    /** Represents a buffer to be flushed. */
    static class FlushedBuffer {
        /** The subpartition id that the buffer belongs to. */
        private final int subpartitionId;

        /** The buffer index within the subpartition. */
        private final int bufferIndex;

        /** The file offset that the buffer begin with. */
        private final long fileOffset;

        FlushedBuffer(int subpartitionId, int bufferIndex, long fileOffset) {
            this.subpartitionId = subpartitionId;
            this.bufferIndex = bufferIndex;
            this.fileOffset = fileOffset;
        }

        int getSubpartitionId() {
            return subpartitionId;
        }

        int getBufferIndex() {
            return bufferIndex;
        }

        long getFileOffset() {
            return fileOffset;
        }
    }

    /**
     * Represents a series of buffers that are:
     *
     * <ul>
     *   <li>From the same subpartition
     *   <li>Logically (i.e. buffer index) consecutive
     *   <li>Physically (i.e. offset in the file) consecutive
     * </ul>
     */
    static class Region {

        /**
         * The region size is fixed, including firstBufferIndex, regionFileOffset and numBuffers.
         */
        static final int REGION_SIZE = Integer.BYTES + Long.BYTES + Integer.BYTES;

        /** The buffer index of first buffer. */
        private final int firstBufferIndex;

        /** The file offset of the region. */
        private final long regionFileOffset;

        /** The number of buffers that the region contains. */
        private final int numBuffers;

        Region(int firstBufferIndex, long regionFileOffset, int numBuffers) {
            this.firstBufferIndex = firstBufferIndex;
            this.regionFileOffset = regionFileOffset;
            this.numBuffers = numBuffers;
        }

        boolean containBuffer(int bufferIndex) {
            return bufferIndex >= firstBufferIndex && bufferIndex < firstBufferIndex + numBuffers;
        }

        long getRegionFileOffset() {
            return regionFileOffset;
        }

        int getNumBuffers() {
            return numBuffers;
        }

        int getFirstBufferIndex() {
            return firstBufferIndex;
        }
    }
}
