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

import org.apache.flink.runtime.io.network.buffer.Buffer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The buffer context to be flushed, including the {@link Buffer}, the buffer index, the
 * subpartition id, the segment id, etc.
 */
public class SpilledBufferContext {

    /** The data buffer. Note that the buffer should not be null. */
    private final Buffer buffer;

    /** The index of buffer. */
    private final int bufferIndex;

    /** The id of subpartition. */
    private final int subpartitionId;

    /** The id of segment. */
    private final int segmentId;

    public SpilledBufferContext(Buffer buffer, int bufferIndex, int subpartitionId, int segmentId) {
        this.buffer = checkNotNull(buffer);
        this.bufferIndex = bufferIndex;
        this.subpartitionId = subpartitionId;
        this.segmentId = segmentId;
    }

    public Buffer getBuffer() {
        return buffer;
    }

    public int getBufferIndex() {
        return bufferIndex;
    }

    public int getSubpartitionId() {
        return subpartitionId;
    }

    public int getSegmentId() {
        return segmentId;
    }
}
