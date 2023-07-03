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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.remote;

import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageConfiguration;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.PartitionFileReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.PartitionFileWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.SegmentPartitionFile;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageResourceRegistry;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierConsumerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierMasterAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierProducerAgent;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageUtils.getTieredStoragePath;

public class RemoteTierFactory implements TierFactory {

    private final int numBytesPerSegment;

    private final int bufferSizeBytes;

    private final String remoteStorageBasePath;

    public RemoteTierFactory(
            int numBytesPerSegment, int bufferSizeBytes, String remoteStorageBasePath) {
        this.numBytesPerSegment = numBytesPerSegment;
        this.bufferSizeBytes = bufferSizeBytes;
        this.remoteStorageBasePath = remoteStorageBasePath;
    }

    @Override
    public TierMasterAgent createMasterAgent(TieredStorageResourceRegistry resourceRegistry) {
        return new RemoteTierMasterAgent(resourceRegistry, remoteStorageBasePath);
    }

    @Override
    public TierProducerAgent createProducerAgent(
            int numSubpartitions,
            TieredStoragePartitionId partitionID,
            String dataFileBasePath,
            boolean isBroadcastOnly,
            BufferCompressor bufferCompressor,
            TieredStorageMemoryManager storageMemoryManager,
            TieredStorageNettyService nettyService,
            BatchShuffleReadBufferPool batchShuffleReadBufferPool,
            ScheduledExecutorService batchShuffleReadIOExecutor,
            TieredStorageConfiguration tieredStorageConfiguration,
            TieredStorageResourceRegistry resourceRegistry) {
        PartitionFileWriter partitionFileWriter =
                SegmentPartitionFile.createPartitionFileWriter(
                        getTieredStoragePath(remoteStorageBasePath), numSubpartitions);
        return new RemoteTierProducerAgent(
                partitionID,
                numSubpartitions,
                numBytesPerSegment,
                bufferSizeBytes,
                isBroadcastOnly,
                bufferCompressor,
                partitionFileWriter,
                storageMemoryManager,
                resourceRegistry);
    }

    @Override
    public TierConsumerAgent createConsumerAgent(
            Map<
                            TieredStoragePartitionId,
                            Map<
                                    TieredStorageSubpartitionId,
                                    CompletableFuture<NettyConnectionReader>>>
                    readers,
            RemoteStorageScanner remoteStorageScanner,
            int remoteBufferSize) {
        PartitionFileReader partitionFileReader =
                SegmentPartitionFile.createPartitionFileReader(
                        getTieredStoragePath(remoteStorageBasePath));
        return new RemoteTierConsumerAgent(
                remoteStorageScanner, partitionFileReader, remoteBufferSize);
    }
}
