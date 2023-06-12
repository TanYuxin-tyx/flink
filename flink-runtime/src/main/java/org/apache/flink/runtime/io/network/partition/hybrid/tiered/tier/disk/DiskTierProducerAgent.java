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

import org.apache.flink.runtime.io.network.api.EndOfSegmentEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyServiceProducer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file.PartitionFileManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file.PartitionFileReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file.PartitionFileType;
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

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.END_OF_SEGMENT;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageUtils.DATA_FILE_SUFFIX;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** The DataManager of LOCAL file. */
public class DiskTierProducerAgent implements TierProducerAgent {

    private final int numBytesPerSegment;

    private final ResultPartitionID resultPartitionID;

    private final Path dataFilePath;

    private final float minReservedDiskSpaceFraction;

    private final PartitionFileReader partitionFileReader;

    private volatile boolean isReleased;

    private final int[] numSubpartitionEmitBytes;

    private final DiskCacheManager diskCacheManager;

    private final List<Map<Integer, Integer>> firstBufferContextInSegment;

    private volatile boolean isClosed;

    public DiskTierProducerAgent(
            TieredStoragePartitionId partitionId,
            int numSubpartitions,
            int numBytesPerSegment,
            ResultPartitionID resultPartitionID,
            String dataFileBasePath,
            float minReservedDiskSpaceFraction,
            boolean isBroadcastOnly,
            PartitionFileManager partitionFileManager,
            TieredStorageMemoryManager storageMemoryManager,
            TieredStorageNettyService nettyService) {
        this.numBytesPerSegment = numBytesPerSegment;
        this.resultPartitionID = resultPartitionID;
        this.dataFilePath = Paths.get(dataFileBasePath + DATA_FILE_SUFFIX);
        this.minReservedDiskSpaceFraction = minReservedDiskSpaceFraction;
        this.firstBufferContextInSegment = new ArrayList<>();
        for (int i = 0; i < numSubpartitions; ++i) {
            firstBufferContextInSegment.add(new ConcurrentHashMap<>());
        }

        this.partitionFileReader =
                partitionFileManager.createPartitionFileReader(
                        PartitionFileType.PRODUCER_MERGE,
                        nettyService,
                        firstBufferContextInSegment);

        this.numSubpartitionEmitBytes = new int[numSubpartitions];
        this.diskCacheManager =
                new DiskCacheManager(
                        isBroadcastOnly ? 1 : numSubpartitions,
                        storageMemoryManager,
                        partitionFileManager);

        nettyService.registerProducer(
                partitionId,
                new NettyServiceProducer() {
                    @Override
                    public void connectionEstablished(
                            TieredStorageSubpartitionId subpartitionId,
                            NettyConnectionWriter nettyConnectionWriter) {
                        DiskTierProducerAgent.this.register(subpartitionId, nettyConnectionWriter);
                    }

                    @Override
                    public void connectionBroken(NettyConnectionId connectionId) {
                        partitionFileReader.releaseReader(connectionId);
                    }
                });
    }

    private void register(
            TieredStorageSubpartitionId subpartitionId,
            NettyConnectionWriter nettyConnectionWriter) {
        if (!Files.isReadable(dataFilePath)) {
            throw new RuntimeException(new PartitionNotFoundException(resultPartitionID));
        }
        try {
            partitionFileReader.registerNettyService(
                    subpartitionId.getSubpartitionId(), nettyConnectionWriter);
        } catch (IOException e) {
            ExceptionUtils.rethrow(e, "Failed to create PartitionFileReader");
        }
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
    public void release() {
        // release is called when release by scheduler, later than close.
        // mainly work :
        // 1. release read scheduler.
        // 2. delete shuffle file.
        // 3. release all data in memory.
        if (!isReleased) {
            partitionFileReader.release();
            getDiskCacheManager().release();
            isReleased = true;
        }
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

    private void emitEndOfSegmentEvent(int targetChannel) {
        try {
            diskCacheManager.appendSegmentEvent(
                    EventSerializer.toSerializedEvent(EndOfSegmentEvent.INSTANCE),
                    targetChannel,
                    END_OF_SEGMENT);
        } catch (IOException e) {
            ExceptionUtils.rethrow(e, "Failed to emitEndOfSegmentEvent");
        }
    }

    private void emitBuffer(Buffer finishedBuffer, int targetSubpartition) {
        diskCacheManager.append(finishedBuffer, targetSubpartition);
    }

    @Override
    public void close() {
        if (!isClosed) {
            // close is called when task is finished or failed.
            checkNotNull(diskCacheManager).close();
            isClosed = true;
        }
    }

    public DiskCacheManager getDiskCacheManager() {
        return diskCacheManager;
    }
}
