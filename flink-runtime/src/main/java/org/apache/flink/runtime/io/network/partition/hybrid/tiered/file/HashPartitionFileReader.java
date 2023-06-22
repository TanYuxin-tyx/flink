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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferHeader;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageIdMappingUtils;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionId;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.parseBufferHeader;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageUtils.generateNewSegmentPath;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageUtils.getBaseSubpartitionPath;

/** THe implementation of {@link PartitionFileReader} with hash logic. */
public class HashPartitionFileReader implements PartitionFileReader {

    private final ByteBuffer reusedHeaderBuffer = BufferReaderWriterUtil.allocatedHeaderBuffer();

    private final Map<
                    TieredStoragePartitionId,
                    Map<TieredStorageSubpartitionId, Tuple2<ReadableByteChannel, Integer>>>
            openedChannels = new HashMap<>();

    private final String basePath;

    private final Boolean isBroadcast;

    private FileSystem fileSystem;

    public HashPartitionFileReader(String basePath, Boolean isBroadcast) {
        this.basePath = basePath;
        this.isBroadcast = isBroadcast;
        try {
            this.fileSystem = new Path(basePath).getFileSystem();
        } catch (IOException e) {
            ExceptionUtils.rethrow(e, "Failed to initialize the FileSystem.");
        }
    }

    @Override
    public Buffer readBuffer(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId,
            int bufferIndex,
            MemorySegment segment,
            BufferRecycler recycler,
            NettyConnectionId nettyConnectionId)
            throws IOException {
        Tuple2<ReadableByteChannel, Integer> fileChannelAndSegmentId =
                openedChannels
                        .computeIfAbsent(partitionId, ignore -> new HashMap<>())
                        .getOrDefault(subpartitionId, Tuple2.of(null, -1));
        ReadableByteChannel channel = fileChannelAndSegmentId.f0;
        if (channel == null || fileChannelAndSegmentId.f1 != segmentId) {
            if (channel != null) {
                channel.close();
            }
            channel = openNewChannel(partitionId, subpartitionId, segmentId);
            if (channel == null) {
                return null;
            }
            openedChannels.get(partitionId).put(subpartitionId, Tuple2.of(channel, segmentId));
        }

        reusedHeaderBuffer.clear();
        int bufferHeaderResult = channel.read(reusedHeaderBuffer);
        if (bufferHeaderResult == -1) {
            channel = null;
            openedChannels.get(partitionId).put(subpartitionId, Tuple2.of(channel, segmentId));
            return new NetworkBuffer(
                    MemorySegmentFactory.allocateUnpooledSegment(0),
                    FreeingBufferRecycler.INSTANCE,
                    Buffer.DataType.END_OF_SEGMENT,
                    0);
        }
        reusedHeaderBuffer.rewind();
        BufferHeader header = parseBufferHeader(reusedHeaderBuffer);
        ByteBuffer dataBuffer = ByteBuffer.allocateDirect(header.getLength());
        int dataBufferResult = channel.read(dataBuffer);
        if (dataBufferResult == -1) {
            throw new IOException("An empty data buffer is read.");
        }
        Buffer.DataType dataType = header.getDataType();
        return new NetworkBuffer(
                MemorySegmentFactory.wrapOffHeapMemory(dataBuffer),
                FreeingBufferRecycler.INSTANCE,
                dataType,
                header.isCompressed(),
                header.getLength());
    }

    @Override
    public long getPriority(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId,
            int bufferIndex,
            NettyConnectionId nettyConnectionId) {
        // noop
        return -1;
    }

    private ReadableByteChannel openNewChannel(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId)
            throws IOException {
        String baseSubpartitionPath =
                getBaseSubpartitionPath(
                        basePath,
                        TieredStorageIdMappingUtils.convertId(partitionId),
                        subpartitionId.getSubpartitionId(),
                        isBroadcast);
        Path currentSegmentPath = generateNewSegmentPath(baseSubpartitionPath, segmentId);
        ReadableByteChannel channel = null;
        if (!fileSystem.exists(currentSegmentPath)) {
            return channel;
        }
        channel = Channels.newChannel(fileSystem.open(currentSegmentPath));
        return channel;
    }

    @Override
    public void release() {}
}
