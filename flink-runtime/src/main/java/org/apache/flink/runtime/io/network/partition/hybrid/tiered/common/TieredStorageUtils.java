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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Utils for reading or writing to tiered store. */
public class TieredStorageUtils {

    public static final String TIER_STORAGE_DIR = "tiered-storage";

    public static final String DATA_FILE_SUFFIX = ".tier-storage.data";

    private static final char[] HEX_CHARS = {
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'
    };

    public static ByteBuffer[] generateBufferWithHeaders(
            List<Tuple2<Buffer, Integer>> bufferWithIndexes) {
        ByteBuffer[] bufferWithHeaders = new ByteBuffer[2 * bufferWithIndexes.size()];

        for (int i = 0; i < bufferWithIndexes.size(); i++) {
            Buffer buffer = bufferWithIndexes.get(i).f0;
            setBufferWithHeader(buffer, bufferWithHeaders, 2 * i);
        }
        return bufferWithHeaders;
    }

    private static void setBufferWithHeader(
            Buffer buffer, ByteBuffer[] bufferWithHeaders, int index) {
        ByteBuffer header = BufferReaderWriterUtil.allocatedHeaderBuffer();
        BufferReaderWriterUtil.setByteChannelBufferHeader(buffer, header);

        bufferWithHeaders[index] = header;
        bufferWithHeaders[index + 1] = buffer.getNioBufferReadable();
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

    public static Buffer useNewBufferRecyclerAndCompressBuffer(
            BufferCompressor bufferCompressor, Buffer originalBuffer, BufferRecycler newRecycler) {
        if (!originalBuffer.isBuffer()) {
            return compressBufferIfPossible(bufferCompressor, originalBuffer);
        }
        NetworkBuffer buffer =
                new NetworkBuffer(
                        originalBuffer.getMemorySegment(),
                        newRecycler,
                        originalBuffer.getDataType(),
                        originalBuffer.getSize());
        return compressBufferIfPossible(bufferCompressor, buffer);
    }

    public static Buffer compressBufferIfPossible(
            BufferCompressor bufferCompressor, Buffer buffer) {
        if (!canBeCompressed(bufferCompressor, buffer)) {
            return buffer;
        }

        return checkNotNull(bufferCompressor).compressToOriginalBuffer(buffer);
    }

    /**
     * Whether the buffer can be compressed or not. Note that event is not compressed because it is
     * usually small and the size can become even larger after compression.
     */
    public static boolean canBeCompressed(BufferCompressor bufferCompressor, Buffer buffer) {
        return bufferCompressor != null && buffer.isBuffer() && buffer.readableBytes() > 0;
    }
}
