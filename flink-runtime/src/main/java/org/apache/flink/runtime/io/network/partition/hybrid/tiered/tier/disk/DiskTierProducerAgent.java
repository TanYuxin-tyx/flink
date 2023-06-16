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
import org.apache.flink.runtime.io.network.api.EndOfSegmentEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageConfiguration;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyServiceProducer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageResourceRegistry;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file.PartitionFileIndex;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file.PartitionFileReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file.PartitionFileWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.ioscheduler.DiskIOScheduler;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierProducerAgent;
import org.apache.flink.util.ExceptionUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.END_OF_SEGMENT;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageUtils.DATA_FILE_SUFFIX;

/** The DataManager of LOCAL file. */
public class DiskTierProducerAgent implements TierProducerAgent, NettyServiceProducer {

    private final int numBytesPerSegment;

    private final ResultPartitionID resultPartitionID;

    private final Path dataFilePath;

    private final float minReservedDiskSpaceFraction;

    private final DiskIOScheduler diskIOScheduler;

    private volatile boolean isReleased;

    private final int[] numSubpartitionEmitBytes;

    private final DiskCacheManager diskCacheManager;

    private final List<Map<Integer, Integer>> firstBufferContextInSegment;

    public DiskTierProducerAgent(
            TieredStoragePartitionId partitionId,
            int numSubpartitions,
            int numBytesPerSegment,
            ResultPartitionID resultPartitionID,
            String dataFileBasePath,
            float minReservedDiskSpaceFraction,
            boolean isBroadcastOnly,
            PartitionFileWriter partitionFileWriter,
            PartitionFileReader partitionFileReader,
            TieredStorageMemoryManager storageMemoryManager,
            TieredStorageNettyService nettyService,
            BatchShuffleReadBufferPool batchShuffleReadBufferPool,
            ScheduledExecutorService batchShuffleReadIOExecutor,
            TieredStorageConfiguration storeConfiguration,
            PartitionFileIndex dataIndex,
            TieredStorageResourceRegistry resourceRegistry) {
        this.numBytesPerSegment = numBytesPerSegment;
        this.resultPartitionID = resultPartitionID;
        this.dataFilePath = Paths.get(dataFileBasePath + DATA_FILE_SUFFIX);
        this.minReservedDiskSpaceFraction = minReservedDiskSpaceFraction;
        this.firstBufferContextInSegment = new ArrayList<>();
        this.numSubpartitionEmitBytes = new int[numSubpartitions];

        for (int i = 0; i < numSubpartitions; ++i) {
            firstBufferContextInSegment.add(new ConcurrentHashMap<>());
        }
        this.diskCacheManager =
                new DiskCacheManager(
                        isBroadcastOnly ? 1 : numSubpartitions,
                        storageMemoryManager,
                        partitionFileWriter);
        this.diskIOScheduler =
                new DiskIOScheduler(
                        batchShuffleReadBufferPool,
                        batchShuffleReadIOExecutor,
                        storeConfiguration.getMaxRequestedBuffers(),
                        storeConfiguration.getBufferRequestTimeout(),
                        storeConfiguration.getMaxBuffersReadAhead(),
                        nettyService,
                        firstBufferContextInSegment,
                        partitionFileReader,
                        dataIndex);
        nettyService.registerProducer(partitionId, this);
        resourceRegistry.registerResource(partitionId, this::releaseResources);
    }

    @Override
    public boolean tryStartNewSegment(
            TieredStorageSubpartitionId subpartitionId,
            int segmentId,
            boolean forceUseCurrentTier) {
        File filePath = dataFilePath.toFile();
        boolean canStartNewSegment =
                filePath.getUsableSpace()
                        > (long) (filePath.getTotalSpace() * minReservedDiskSpaceFraction);
        if (canStartNewSegment || forceUseCurrentTier) {
            firstBufferContextInSegment
                    .get(subpartitionId.getSubpartitionId())
                    .put(
                            diskCacheManager.getFinishedBufferIndex(
                                    subpartitionId.getSubpartitionId()),
                            segmentId);
        }
        return canStartNewSegment || forceUseCurrentTier;
    }

    @Override
    public boolean tryWrite(int consumerId, Buffer finishedBuffer) {
        if (numSubpartitionEmitBytes[consumerId] != 0
                && numSubpartitionEmitBytes[consumerId] + finishedBuffer.readableBytes()
                        > numBytesPerSegment) {
            emitEndOfSegmentEvent(consumerId);
            numSubpartitionEmitBytes[consumerId] = 0;
            return false;
        }
        numSubpartitionEmitBytes[consumerId] += finishedBuffer.readableBytes();
        emitBuffer(finishedBuffer, consumerId);
        return true;
    }

    @Override
    public void connectionEstablished(
            TieredStorageSubpartitionId subpartitionId,
            NettyConnectionWriter nettyConnectionWriter) {
        if (!Files.isReadable(dataFilePath)) {
            throw new RuntimeException(new PartitionNotFoundException(resultPartitionID));
        }
        diskIOScheduler.connectionEstablished(subpartitionId, nettyConnectionWriter);
    }

    @Override
    public void connectionBroken(NettyConnectionId connectionId) {
        diskIOScheduler.connectionBroken(connectionId);
    }

    @Override
    public void close() {
        diskCacheManager.close();
    }

    @Override
    public void release() {}

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    private void emitEndOfSegmentEvent(int subpartitionId) {
        try {
            diskCacheManager.appendEndOfSegmentEvent(
                    EventSerializer.toSerializedEvent(EndOfSegmentEvent.INSTANCE),
                    subpartitionId,
                    END_OF_SEGMENT);
        } catch (IOException e) {
            ExceptionUtils.rethrow(e, "Failed to emitEndOfSegmentEvent");
        }
    }

    private void emitBuffer(Buffer finishedBuffer, int subpartition) {
        diskCacheManager.append(finishedBuffer, subpartition);
    }

    private void releaseResources() {
        if (!isReleased) {
            diskIOScheduler.release();
            diskCacheManager.release();
            isReleased = true;
        }
    }
}
