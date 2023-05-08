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
 * The {@link BufferContext} represents a combination of buffer, buffer index, and its subpartition
 * id, and it could also indicate a error.
 */
public class BufferContext {

    private Buffer buffer;

    private Throwable error;

    private int bufferIndex;

    private int subpartitionId;

    // If the buffer is not null, the error must be null.
    public BufferContext(Buffer buffer, int bufferIndex, int subpartitionId) {
        this.buffer = buffer;
        this.bufferIndex = bufferIndex;
        this.subpartitionId = subpartitionId;
    }

    // If the error is not null, other elements must be null.
    public BufferContext(Throwable error) {
        this.error = error;
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
}
