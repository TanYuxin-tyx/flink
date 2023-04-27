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

    @Nullable private Buffer buffer;

    @Nullable private Throwable throwable;

    private int bufferIndex;

    private int subpartitionId;

    public BufferContext(@Nullable Buffer buffer, int bufferIndex, int subpartitionId) {
        this.buffer = buffer;
        this.bufferIndex = bufferIndex;
        this.subpartitionId = subpartitionId;
    }

    public BufferContext(@Nullable Throwable throwable) {
        this.throwable = throwable;
    }

    @Nullable
    public Buffer getBuffer() {
        return buffer;
    }

    @Nullable
    public Throwable getThrowable() {
        return throwable;
    }

    public int getBufferIndex() {
        return bufferIndex;
    }

    public int getSubpartitionId() {
        return subpartitionId;
    }
}
