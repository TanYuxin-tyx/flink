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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageIdMappingUtils;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.HashMap;
import java.util.Map;

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

    private final JobID jobID;

    private final Boolean isUpstreamBroadcast;

    private FileSystem fileSystem;

    HashPartitionFileReader(String basePath, JobID jobID, Boolean isUpstreamBroadCast) {
        this.basePath = basePath;
        this.jobID = jobID;
        this.isUpstreamBroadcast = isUpstreamBroadCast;
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
            long fileOffSet,
            MemorySegment segment,
            BufferRecycler recycler)
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
            openedChannels.get(partitionId).put(subpartitionId, Tuple2.of(channel, segmentId));
        }
        return null;
    }

    private ReadableByteChannel openNewChannel(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId) {
        String baseSubpartitionPath =
                getBaseSubpartitionPath(
                        jobID,
                        TieredStorageIdMappingUtils.convertId(partitionId),
                        subpartitionId.getSubpartitionId(),
                        basePath,
                        isUpstreamBroadcast);
        Path currentSegmentPath = generateNewSegmentPath(baseSubpartitionPath, segmentId);
        ReadableByteChannel channel = null;
        try {
            channel = Channels.newChannel(fileSystem.open(currentSegmentPath));
        } catch (IOException e) {
            ExceptionUtils.rethrow(e, "Failed to create file channel.");
        }
        return channel;
    }

    @Override
    public void release() {}
}
