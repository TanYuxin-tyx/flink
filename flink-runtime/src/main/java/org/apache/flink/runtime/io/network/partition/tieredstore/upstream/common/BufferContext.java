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

import org.apache.flink.runtime.io.network.buffer.Buffer;

/** The {@link BufferContext} is used in Tiered Store. */
public class BufferContext {

    private final Buffer buffer;

    private final BufferIndexAndChannel bufferIndexAndChannel;

    private final boolean isLastBufferInSegment;

    public BufferContext(
            Buffer buffer, int bufferIndex, int subpartitionId, boolean isLastBufferInSegment) {
        this.bufferIndexAndChannel = new BufferIndexAndChannel(bufferIndex, subpartitionId);
        this.buffer = buffer;
        this.isLastBufferInSegment = isLastBufferInSegment;
    }

    public Buffer getBuffer() {
        return buffer;
    }

    public BufferIndexAndChannel getBufferIndexAndChannel() {
        return bufferIndexAndChannel;
    }

    public boolean isLastBufferInSegment() {
        return isLastBufferInSegment;
    }
}
