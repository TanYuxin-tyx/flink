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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage;

import org.apache.flink.runtime.io.network.buffer.Buffer;

import javax.annotation.Nullable;

/**
 * The {@link NettyPayload} represents the payload that will be transferred to netty connection. It
 * could indicate a combination of buffer, buffer index, and its subpartition id, and it could also
 * indicate an error or a segment id.
 */
public class NettyPayload {

    // If the buffer is not null, bufferIndex and subpartitionId will be non-negative, error will be
    // null, segmentId will be -1;
    private Buffer buffer;

    // If the error is not null, buffer will be null, segmentId and bufferIndex and subpartitionId
    // will be -1.
    private Throwable error;

    // If the bufferIndex is non-negative, buffer won't be null, error will be null, subpartitionId
    // will be non-negative, segmentId will be -1.
    private int bufferIndex = -1;

    // If the subpartitionId is non-negative, buffer won't be null, error will be null, bufferIndex
    // will be non-negative, segmentId will be -1.
    private int subpartitionId = -1;

    // If the segmentId is non-negative, buffer and error be null, bufferIndex and subpartitionId
    // will be -1.
    private int segmentId = -1;

    public NettyPayload(Buffer buffer, int bufferIndex, int subpartitionId) {
        this.buffer = buffer;
        this.bufferIndex = bufferIndex;
        this.subpartitionId = subpartitionId;
    }

    public NettyPayload(Throwable error) {
        this.error = error;
    }

    public NettyPayload(int segmentId) {
        this.segmentId = segmentId;
    }

    @Nullable
    public Buffer getBuffer() {
        return buffer;
    }

    @Nullable
    public Throwable getError() {
        return error;
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
