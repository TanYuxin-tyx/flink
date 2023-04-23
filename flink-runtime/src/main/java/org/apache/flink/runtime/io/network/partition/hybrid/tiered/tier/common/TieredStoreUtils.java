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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.common;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TierType;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.remote.RemoteTierProducerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.common.BufferContext;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.common.CacheBufferFlushTrigger;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.common.CacheFlushManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.common.file.PartitionFileManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.local.disk.DiskTierProducerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.local.memory.MemoryTierProducerAgent;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkState;

/** Utils for reading or writing to tiered store. */
public class TieredStoreUtils {

    public static final String TIER_STORE_DIR = "tiered-store";

    public static final String DATA_FILE_SUFFIX = ".store.data";

    private static final String SEGMENT_FILE_PREFIX = "seg-";

    private static final String SEGMENT_FINISH_FILE_SUFFIX = ".FINISH";

    public static ByteBuffer[] generateBufferWithHeaders(List<BufferContext> bufferContexts) {
        ByteBuffer[] bufferWithHeaders = new ByteBuffer[2 * bufferContexts.size()];

        for (int i = 0; i < bufferContexts.size(); i++) {
            Buffer buffer = bufferContexts.get(i).getBuffer();
            setBufferWithHeader(buffer, bufferWithHeaders, 2 * i);
        }
        return bufferWithHeaders;
    }

    public static void setBufferWithHeader(
            Buffer buffer, ByteBuffer[] bufferWithHeaders, int index) {
        ByteBuffer header = BufferReaderWriterUtil.allocatedHeaderBuffer();
        BufferReaderWriterUtil.setByteChannelBufferHeader(buffer, header);

        bufferWithHeaders[index] = header;
        bufferWithHeaders[index + 1] = buffer.getNioBufferReadable();
    }

    public static void writeDfsBuffers(
            WritableByteChannel writeChannel, long expectedBytes, ByteBuffer[] bufferWithHeaders)
            throws IOException {
        int writeSize = 0;
        for (ByteBuffer bufferWithHeader : bufferWithHeaders) {
            writeSize += writeChannel.write(bufferWithHeader);
        }
        checkState(writeSize == expectedBytes);
    }

    public static void checkFlushCacheBuffers(
            TieredStorageMemoryManager storageMemoryManager,
            CacheBufferFlushTrigger cacheBufferFlushTrigger,
            float numBuffersTriggerFlushRatio) {
        if (needFlushCacheBuffers(storageMemoryManager, numBuffersTriggerFlushRatio)) {
            cacheBufferFlushTrigger.notifyFlushCachedBuffers();
        }
    }

    public static boolean needFlushCacheBuffers(
            TieredStorageMemoryManager tieredStoreMemoryManager,
            float numBuffersTriggerFlushRatio) {
        int numTotal = tieredStoreMemoryManager.numTotalBuffers();
        int numRequested = tieredStoreMemoryManager.numRequestedBuffers();
        return numRequested >= numTotal
                || (numRequested * 1.0 / numTotal) >= numBuffersTriggerFlushRatio;
    }

    public static List<TierProducerAgent> createTierProducerAgents(
            ResultPartitionID id,
            boolean isBroadcast,
            BufferCompressor bufferCompressor,
            ResultSubpartition[] subpartitions,
            TieredStoreConfiguration storeConfiguration,
            TieredStorageMemoryManager storeMemoryManager,
            CacheFlushManager cacheFlushManager,
            String dataFileBasePath,
            int networkBufferSize,
            float minReservedDiskSpaceFraction,
            PartitionFileManager partitionFileManager) {
        List<TierProducerAgent> tierProducerAgents = new ArrayList<>();
        int i = 0;
        for (TierConfSpec tierConfSpec : storeConfiguration.getTierConfSpecs()) {
            if (tierConfSpec.getTierType() == TierType.IN_MEM) {
                tierProducerAgents.add(
                        new MemoryTierProducerAgent(
                                i,
                                subpartitions.length,
                                storeMemoryManager,
                                isBroadcast,
                                bufferCompressor,
                                networkBufferSize));
            } else if (tierConfSpec.getTierType() == TierType.IN_DISK) {
                tierProducerAgents.add(
                        new DiskTierProducerAgent(
                                i,
                                subpartitions.length,
                                id,
                                dataFileBasePath,
                                minReservedDiskSpaceFraction,
                                isBroadcast,
                                partitionFileManager,
                                networkBufferSize,
                                storeMemoryManager,
                                bufferCompressor,
                                cacheFlushManager));
            } else if (tierConfSpec.getTierType() == TierType.IN_REMOTE) {
                RemoteTierProducerAgent remoteTierProducerAgent =
                        new RemoteTierProducerAgent(
                                subpartitions.length,
                                isBroadcast,
                                networkBufferSize,
                                storeMemoryManager,
                                cacheFlushManager,
                                bufferCompressor,
                                partitionFileManager);
                remoteTierProducerAgent.setTierIndex(i);
                tierProducerAgents.add(remoteTierProducerAgent);
            }
            i++;
        }
        return tierProducerAgents;
    }

    public static String createBaseSubpartitionPath(
            JobID jobID,
            ResultPartitionID resultPartitionID,
            int subpartitionId,
            String baseDfsPath,
            boolean isBroadcastOnly)
            throws IOException {
        String basePathStr =
                getBaseSubpartitionPath(
                        jobID, resultPartitionID, subpartitionId, baseDfsPath, isBroadcastOnly);
        Path basePath = new Path(basePathStr);
        FileSystem fs = basePath.getFileSystem();
        if (!fs.exists(basePath)) {
            fs.mkdirs(basePath);
        }
        return basePathStr;
    }

    public static String getBaseSubpartitionPath(
            JobID jobID,
            ResultPartitionID resultPartitionID,
            int subpartitionId,
            String baseDfsPath,
            boolean isBroadcastOnly) {
        while (baseDfsPath.endsWith("/") && baseDfsPath.length() > 1) {
            baseDfsPath = baseDfsPath.substring(0, baseDfsPath.length() - 1);
        }
        if (isBroadcastOnly) {
            subpartitionId = 0;
        }
        return String.format(
                "%s/%s/%s/%s/%s",
                baseDfsPath, TIER_STORE_DIR, jobID, resultPartitionID, subpartitionId);
    }

    public static Path generateToReleasePath(JobID jobID, String baseDfsPath) {
        if (jobID == null || baseDfsPath == null) {
            return null;
        }

        while (baseDfsPath.endsWith("/") && baseDfsPath.length() > 1) {
            baseDfsPath = baseDfsPath.substring(0, baseDfsPath.length() - 1);
        }
        String basePathStr = String.format("%s/%s/%s", baseDfsPath, TIER_STORE_DIR, jobID);
        return new Path(basePathStr);
    }

    public static void deletePath(Path path) throws IOException {
        if (path == null) {
            return;
        }
        FileSystem fs = path.getFileSystem();
        if (fs.exists(path)) {
            fs.delete(path, true);
        }
    }

    public static String deleteJobBasePath(JobID jobID, String baseDfsPath) throws IOException {
        while (baseDfsPath.endsWith("/") && baseDfsPath.length() > 1) {
            baseDfsPath = baseDfsPath.substring(0, baseDfsPath.length() - 1);
        }
        String basePathStr = String.format("%s/%s/%s", baseDfsPath, TIER_STORE_DIR, jobID);
        Path basePath = new Path(basePathStr);
        FileSystem fs = basePath.getFileSystem();
        if (fs.exists(basePath)) {
            fs.delete(basePath, true);
        }
        return basePathStr;
    }

    public static Path generateNewSegmentPath(
            String baseSubpartitionPath, long currentSegmentIndex) {
        return new Path(baseSubpartitionPath, "/" + SEGMENT_FILE_PREFIX + currentSegmentIndex);
    }

    public static Path generateSegmentFinishPath(
            String baseSubpartitionPath, long currentSegmentIndex) {
        return new Path(
                baseSubpartitionPath,
                "/" + SEGMENT_FILE_PREFIX + currentSegmentIndex + SEGMENT_FINISH_FILE_SUFFIX);
    }

    public static void writeSegmentFinishFile(
            String baseSubpartitionPath, long currentSegmentIndex) {
        Path markFinishSegmentPath =
                generateSegmentFinishPath(baseSubpartitionPath, currentSegmentIndex);
        try {
            FileSystem fs = markFinishSegmentPath.getFileSystem();
            OutputStream outputStream =
                    fs.create(markFinishSegmentPath, FileSystem.WriteMode.OVERWRITE);
            outputStream.close();
        } catch (IOException e) {
            ExceptionUtils.rethrow(e);
        }
    }
}
