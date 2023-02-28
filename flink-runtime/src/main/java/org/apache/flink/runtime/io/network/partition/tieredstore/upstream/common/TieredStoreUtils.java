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

package org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkState;

/** Utils for reading or writing to tiered store. */
public class TieredStoreUtils {

    private static final String TIER_STORE_DIR = "tiered-store";

    private static final String SEGMENT_FILE_PREFIX = "seg-";

    private static final String SEGMENT_FINISH_FILE_SUFFIX = ".FINISH";

    private static final float NUM_TRIGGER_FLUSH_RATIO = 0.8f;

    public static ByteBuffer[] generateBufferWithHeaders(
            List<BufferWithIdentity> bufferWithIdentities) {
        ByteBuffer[] bufferWithHeaders = new ByteBuffer[2 * bufferWithIdentities.size()];

        for (int i = 0; i < bufferWithIdentities.size(); i++) {
            Buffer buffer = bufferWithIdentities.get(i).getBuffer();
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
            BufferPoolHelper bufferPoolHelper, CacheBufferSpillTrigger cacheBufferSpillTrigger) {
        int numAvailable = bufferPoolHelper.numAvailableBuffers();
        int numTotal = bufferPoolHelper.numTotalBuffers();
        if ((numAvailable * 1.0 / numTotal) > NUM_TRIGGER_FLUSH_RATIO) {
            cacheBufferSpillTrigger.notifyFlushCachedBuffers();
        }
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