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
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.util.HashMap;
import java.util.Map;

/** THe implementation of {@link PartitionFileReader} with hash logic. */
public class HashPartitionFileReader implements PartitionFileReader {

    private final ByteBuffer reusedHeaderBuffer = BufferReaderWriterUtil.allocatedHeaderBuffer();

    private final Map<TieredStoragePartitionId, Map<TieredStorageSubpartitionId, Channel>>
            openedFileChannels = new HashMap<>();

    private final String basePath;

    HashPartitionFileReader(String basePath) {
        this.basePath = basePath;
    }

    @Override
    public Buffer readBuffer(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId,
            long fileOffSet,
            MemorySegment segment,
            BufferRecycler recycler)
            throws IOException {
        Channel fileChannel = openedFileChannels
                .computeIfAbsent(partitionId, ignore -> new HashMap<>())
                .getOrDefault(subpartitionId, null);


        //Buffer buffer = null;
        //try {
        //    buffer = readFromByteChannel(fileChannel, reusedHeaderBuffer, segment, recycler);
        //} catch (IOException e) {
        //    ExceptionUtils.rethrow(e, "Failed to read buffer.");
        //}
        //return buffer;
        return null;
    }

    @Override
    public void release() {}
}
