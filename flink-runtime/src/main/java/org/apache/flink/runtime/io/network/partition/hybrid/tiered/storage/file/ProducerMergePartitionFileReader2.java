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
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.readFromByteChannel;

public class ProducerMergePartitionFileReader2 implements PartitionFileReader2 {

    private final ByteBuffer reusedHeaderBuffer = BufferReaderWriterUtil.allocatedHeaderBuffer();

    private final Path dataFilePath;

    private FileChannel fileChannel;

    public ProducerMergePartitionFileReader2(Path dataFilePath) {
        this.dataFilePath = dataFilePath;
    }

    @Override
    public Buffer readBuffer(
            int subpartitionId,
            long fileOffset,
            MemorySegment memorySegment,
            BufferRecycler recycler) {

        try {
            if (fileChannel == null) {
                fileChannel = FileChannel.open(dataFilePath, StandardOpenOption.READ);
            }
            fileChannel.position(fileOffset);
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize the file channel.", e);
        }
        Buffer buffer = null;
        try {
            buffer = readFromByteChannel(fileChannel, reusedHeaderBuffer, memorySegment, recycler);
        } catch (IOException e) {
            ExceptionUtils.rethrow(e, "Failed to read buffer.");
        }
        return buffer;
    }

    @Override
    public void release() {
        try {
            fileChannel.close();
        } catch (IOException e) {
            throw new RuntimeException("Failed to close file channel.", e);
        }
    }
}
