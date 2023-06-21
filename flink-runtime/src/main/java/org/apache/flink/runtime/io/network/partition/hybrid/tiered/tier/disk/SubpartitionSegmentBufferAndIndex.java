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

import org.apache.flink.runtime.io.network.buffer.Buffer;

public class SubpartitionSegmentBufferAndIndex {
    /**
     * The data buffer. If the buffer is not null, bufferIndex and subpartitionId will be
     * non-negative, error will be null, segmentId will be -1;
     */
    private final Buffer buffer;

    /**
     * The index of buffer. If the buffer index is non-negative, buffer won't be null, error will be
     * null, subpartitionId will be non-negative, segmentId will be -1.
     */
    private final int bufferIndex;

    /**
     * The id of subpartition. If the subpartition id is non-negative, buffer won't be null, error
     * will be null, bufferIndex will be non-negative, segmentId will be -1.
     */
    private final int subpartitionId;

    /**
     * The id of segment. If the segment id is non-negative, buffer and error be null, bufferIndex
     * and subpartitionId will be -1.
     */
    private final int segmentId;

    public SubpartitionSegmentBufferAndIndex(
            Buffer buffer, int bufferIndex, int subpartitionId, int segmentId) {
        this.buffer = buffer;
        this.bufferIndex = bufferIndex;
        this.subpartitionId = subpartitionId;
        this.segmentId = segmentId;
    }
}
