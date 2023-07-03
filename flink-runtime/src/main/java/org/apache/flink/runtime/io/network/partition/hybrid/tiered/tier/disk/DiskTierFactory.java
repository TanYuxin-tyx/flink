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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.disk;

import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageConfiguration;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.ProducerMergedPartitionFile;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.ProducerMergedPartitionFileIndex;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.ProducerMergedPartitionFileReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.ProducerMergedPartitionFileWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageResourceRegistry;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierConsumerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierMasterAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierProducerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.remote.RemoteStorageScanner;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageUtils.DATA_FILE_SUFFIX;

public class DiskTierFactory implements TierFactory {

    private final int numBytesPerSegment;

    private final int bufferSizeBytes;

    private final float minReservedDiskSpaceFraction;

    public DiskTierFactory(
            int numBytesPerSegment, int bufferSizeBytes, float minReservedDiskSpaceFraction) {
        this.numBytesPerSegment = numBytesPerSegment;
        this.bufferSizeBytes = bufferSizeBytes;
        this.minReservedDiskSpaceFraction = minReservedDiskSpaceFraction;
    }

    @Override
    public TierMasterAgent createMasterAgent(TieredStorageResourceRegistry resourceRegistry) {
        return new DiskTierMasterAgent();
    }

    @Override
    public TierProducerAgent createProducerAgent(
            int numSubpartitions,
            TieredStoragePartitionId partitionId,
            String dataFileBasePath,
            boolean isBroadcastOnly,
            BufferCompressor bufferCompressor,
            TieredStorageMemoryManager storageMemoryManager,
            TieredStorageNettyService nettyService,
            BatchShuffleReadBufferPool batchShuffleReadBufferPool,
            ScheduledExecutorService batchShuffleReadIOExecutor,
            TieredStorageConfiguration storageConfiguration,
            TieredStorageResourceRegistry resourceRegistry) {
        ProducerMergedPartitionFileIndex partitionFileIndex =
                new ProducerMergedPartitionFileIndex(isBroadcastOnly ? 1 : numSubpartitions);
        Path dataFilePath = Paths.get(dataFileBasePath + DATA_FILE_SUFFIX);
        ProducerMergedPartitionFileWriter partitionFileWriter =
                ProducerMergedPartitionFile.createPartitionFileWriter(
                        dataFilePath, partitionFileIndex);
        ProducerMergedPartitionFileReader partitionFileReader =
                ProducerMergedPartitionFile.createPartitionFileReader(
                        dataFilePath, partitionFileIndex);
        return new DiskTierProducerAgent(
                partitionId,
                numSubpartitions,
                numBytesPerSegment,
                bufferSizeBytes,
                dataFileBasePath,
                minReservedDiskSpaceFraction,
                isBroadcastOnly,
                partitionFileWriter,
                partitionFileReader,
                bufferCompressor,
                storageMemoryManager,
                nettyService,
                batchShuffleReadBufferPool,
                batchShuffleReadIOExecutor,
                storageConfiguration,
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
        return new DiskTierConsumerAgent(readers);
    }
}
