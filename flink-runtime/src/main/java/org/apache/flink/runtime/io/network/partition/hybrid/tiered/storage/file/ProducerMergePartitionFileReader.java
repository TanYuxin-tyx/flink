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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageIdMappingUtils;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IOUtils;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.readFromByteChannel;

/** THe implementation of {@link PartitionFileReader} with merged logic. */
public class ProducerMergePartitionFileReader implements PartitionFileReader {

    private final ByteBuffer reusedHeaderBuffer = BufferReaderWriterUtil.allocatedHeaderBuffer();

    private final Path dataFilePath;

    private final TieredStoragePartitionId partitionId;

    @Nullable private FileChannel fileChannel;

    public ProducerMergePartitionFileReader(
            Path dataFilePath, TieredStoragePartitionId partitionId) {
        this.dataFilePath = dataFilePath;
        this.partitionId = partitionId;
    }

    @Override
    public Buffer readBuffer(
            int subpartitionId,
            int segmentId,
            long fileOffSet,
            MemorySegment segment,
            BufferRecycler recycler)
            throws IOException {
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
        return buffer;
    }

    @Override
    public void release() {
        fileChannel = null;
        IOUtils.deleteFileQuietly(dataFilePath);
    }
}
