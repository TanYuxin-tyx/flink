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

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionId;

import java.util.List;
import java.util.Optional;

/**
 * The {@link PartitionFileIndex} represents the indexes and the regions of the spilled buffers. For
 * each spilled data buffer, this maintains the subpartition it belongs to, the buffer index within
 * the subpartition, the offset in file it begin with. The {@link Region} represents a series of
 * physically continuous buffers in the file, which are from the same subpartition.
 */
public interface PartitionFileIndex {

    void addRegionIndex(List<SpilledBuffer> spilledBuffers);

    Optional<PartitionFileIndexImpl.Region> getRegionIndex(
            int subpartitionId, int bufferIndex, NettyConnectionId nettyServiceWriterId);

    void release();

    /** Represents a buffer to be spilled. */
    class SpilledBuffer {
        /** The subpartition id that the buffer belongs to. */
        public final int subpartitionId;

        /** The buffer index within the subpartition. */
        public final int bufferIndex;

        /** The file offset that the buffer begin with. */
        public final long fileOffset;

        public SpilledBuffer(int subpartitionId, int bufferIndex, long fileOffset) {
            this.subpartitionId = subpartitionId;
            this.bufferIndex = bufferIndex;
            this.fileOffset = fileOffset;
        }
    }

    /**
     * A {@link Region} represents a series of physically continuous buffers in the file, which are
     * from the same subpartition.
     */
    class Region {

        /** The first buffer index of the region. */
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

        public boolean containBuffer(int bufferIndex) {
            return bufferIndex >= firstBufferIndex && bufferIndex < firstBufferIndex + numBuffers;
        }

        public long getRegionFileOffset() {
            return regionFileOffset;
        }

        public int getNumBuffers() {
            return numBuffers;
        }
    }
}
