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
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;

import java.io.IOException;

/** {@link PartitionFileReader} defines the read logic for different types of shuffle files. */
public interface PartitionFileReader {

    /**
     * Read a buffer from partition file.
     *
     * @param partitionId partition id indicates the id of partition.
     * @param subpartitionId subpartition id indicates the id of subpartition.
     * @param segmentId segment id indicates the id of segment.
     * @param bufferIndex the index of buffer.
     * @param memorySegment memory segment indicates an empty buffer.
     * @param recycler recycler indicates the owner of the buffer.
     * @return the buffer.
     */
    Buffer readBuffer(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId,
            int bufferIndex,
            MemorySegment memorySegment,
            BufferRecycler recycler)
            throws IOException;

    /**
     * Get the priority when reading a specific partition and subpartition. The priority can be the
     * file offset to achieve sequential disk reads, and the priority can be the same number if
     * there is no priority read requirement.
     *
     * @param partitionId partition id indicates the id of partition.
     * @param subpartitionId subpartition id indicates the id of subpartition.
     * @param segmentId segment id indicates the id of segment.
     * @param bufferIndex the index of buffer.
     * @return the priority.
     */
    long getPriority(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId,
            int bufferIndex);

    /** Release the {@link PartitionFileReader}. */
    void release();
}
