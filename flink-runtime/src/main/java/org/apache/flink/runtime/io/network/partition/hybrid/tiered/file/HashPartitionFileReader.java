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
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferHeader;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.parseBufferHeader;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageUtils.getSegmentPath;

/** THe implementation of {@link PartitionFileReader} with hash mode. */
public class HashPartitionFileReader implements PartitionFileReader {

    private final ByteBuffer reusedHeaderBuffer = BufferReaderWriterUtil.allocatedHeaderBuffer();

    /**
     * Opened file channels and segment id of related segment files stored in map.
     *
     * <p>The key is partition id and subpartition id. The value is file channel and segment id.
     */
    private final Map<
                    TieredStoragePartitionId,
                    Map<TieredStorageSubpartitionId, Tuple2<ReadableByteChannel, Integer>>>
            openedChannelAndSegmentIds = new HashMap<>();

    private final String basePath;

    private FileSystem fileSystem;

    public HashPartitionFileReader(String basePath) {
        this.basePath = basePath;
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
            MemorySegment memorySegment,
            BufferRecycler recycler)
            throws IOException {
        Tuple2<ReadableByteChannel, Integer> fileChannelAndSegmentId =
                openedChannelAndSegmentIds
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
            openedChannelAndSegmentIds
                    .get(partitionId)
                    .put(subpartitionId, Tuple2.of(channel, segmentId));
        }

        reusedHeaderBuffer.clear();
        int bufferHeaderResult = channel.read(reusedHeaderBuffer);
        if (bufferHeaderResult == -1) {
            channel.close();
            channel = null;
            openedChannelAndSegmentIds
                    .get(partitionId)
                    .put(subpartitionId, Tuple2.of(channel, segmentId));
            return new NetworkBuffer(memorySegment, recycler, Buffer.DataType.END_OF_SEGMENT);
        }
        reusedHeaderBuffer.rewind();
        BufferHeader header = parseBufferHeader(reusedHeaderBuffer);
        int dataBufferResult = channel.read(memorySegment.wrap(0, header.getLength()));
        if (dataBufferResult == -1) {
            throw new IOException("Empty data buffer is read.");
        }
        Buffer.DataType dataType = header.getDataType();
        return new NetworkBuffer(
                memorySegment, recycler, dataType, header.isCompressed(), header.getLength());
    }

    @Override
    public long getPriority(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId,
            int bufferIndex) {
        // noop
        return -1;
    }

    private ReadableByteChannel openNewChannel(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId)
            throws IOException {
        Path currentSegmentPath =
                getSegmentPath(
                        basePath, partitionId, subpartitionId.getSubpartitionId(), segmentId);
        if (!fileSystem.exists(currentSegmentPath)) {
            return null;
        }
        return Channels.newChannel(fileSystem.open(currentSegmentPath));
    }

    @Override
    public void release() {
        openedChannelAndSegmentIds.values().stream()
                .map(Map::values)
                .flatMap(
                        (Function<
                                        Collection<Tuple2<ReadableByteChannel, Integer>>,
                                        Stream<Tuple2<ReadableByteChannel, Integer>>>)
                                Collection::stream)
                .filter(Objects::nonNull)
                .forEach(
                        channel -> {
                            try {
                                channel.f0.close();
                            } catch (IOException e) {
                                ExceptionUtils.rethrow(e);
                            }
                        });
    }
}
