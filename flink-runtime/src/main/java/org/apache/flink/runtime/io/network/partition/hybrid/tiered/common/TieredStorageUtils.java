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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.common;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.SpilledBufferContext;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyPayload;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/** Utils for reading or writing to tiered store. */
public class TieredStorageUtils {

    public static final String TIER_STORE_DIR = "tiered-storage";

    public static final String DATA_FILE_SUFFIX = ".storage.data";

    private static final String SEGMENT_FILE_PREFIX = "seg-";

    private static final String SEGMENT_FINISH_FILE_SUFFIX = ".FINISH";

    private static final char[] HEX_CHARS = {
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'
    };

    public static ByteBuffer[] generateBufferWithHeaders(List<SpilledBufferContext> nettyPayloads) {
        ByteBuffer[] bufferWithHeaders = new ByteBuffer[2 * nettyPayloads.size()];

        for (int i = 0; i < nettyPayloads.size(); i++) {
            Buffer buffer = nettyPayloads.get(i).getBuffer();
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

    public static List<SpilledBufferContext> convertToSpilledBufferContext(
            List<NettyPayload> nettyPayloads) {
        return nettyPayloads.stream()
                .filter(nettyPayload -> nettyPayload.getBuffer().isPresent())
                .map(
                        nettyPayload ->
                                new SpilledBufferContext(
                                        nettyPayload.getBuffer().get(),
                                        nettyPayload.getBufferIndex(),
                                        nettyPayload.getSubpartitionId(),
                                        nettyPayload.getSegmentId()))
                .collect(Collectors.toList());
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

    public static String generateToReleaseJobPath(JobID jobID, String baseDfsPath) {
        if (jobID == null || baseDfsPath == null) {
            return null;
        }

        while (baseDfsPath.endsWith("/") && baseDfsPath.length() > 1) {
            baseDfsPath = baseDfsPath.substring(0, baseDfsPath.length() - 1);
        }
        return String.format("%s/%s/%s", baseDfsPath, TIER_STORE_DIR, jobID);
    }

    public static String generateToReleasePartitionPath(
            JobID jobID, ResultPartitionID resultPartitionID, String baseDfsPath) {
        if (jobID == null || baseDfsPath == null) {
            return null;
        }

        while (baseDfsPath.endsWith("/") && baseDfsPath.length() > 1) {
            baseDfsPath = baseDfsPath.substring(0, baseDfsPath.length() - 1);
        }
        return String.format("%s/%s/%s/%s", baseDfsPath, TIER_STORE_DIR, jobID, resultPartitionID);
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

    public static byte[] randomBytes(int length) {
        checkArgument(length > 0, "Must be positive.");

        Random random = new Random();
        byte[] bytes = new byte[length];
        random.nextBytes(bytes);
        return bytes;
    }

    public static String bytesToHexString(byte[] bytes) {
        checkArgument(bytes != null, "Must be not null.");

        char[] chars = new char[bytes.length * 2];

        for (int i = 0; i < chars.length; i += 2) {
            int index = i >>> 1;
            chars[i] = HEX_CHARS[(0xF0 & bytes[index]) >>> 4];
            chars[i + 1] = HEX_CHARS[0x0F & bytes[index]];
        }

        return new String(chars);
    }
}
