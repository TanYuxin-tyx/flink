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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;

public class MemorySegmentAndChannel {
    private final MemorySegment buffer;

    private final int channelIndex;

    private final Buffer.DataType dataType;

    private final int dataSize;

    public MemorySegmentAndChannel(
            MemorySegment buffer, int channelIndex, Buffer.DataType dataType, int dataSize) {
        this.buffer = buffer;
        this.channelIndex = channelIndex;
        this.dataType = dataType;
        this.dataSize = dataSize;
    }

    public MemorySegment getBuffer() {
        return buffer;
    }

    public int getChannelIndex() {
        return channelIndex;
    }

    public Buffer.DataType getDataType() {
        return dataType;
    }

    public int getDataSize() {
        return dataSize;
    }
}