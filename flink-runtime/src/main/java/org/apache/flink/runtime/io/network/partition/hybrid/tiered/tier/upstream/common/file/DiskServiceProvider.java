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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.common.file;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.common.BufferIndexOrError;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.service.NettyServiceProvider;

import java.util.Deque;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

public class DiskServiceProvider implements NettyServiceProvider {

    private final Deque<BufferIndexOrError> loadedBuffers;

    private final Runnable diskServiceReleaser;

    public DiskServiceProvider(
            Deque<BufferIndexOrError> loadedBuffers, Runnable diskServiceReleaser) {
        this.loadedBuffers = loadedBuffers;
        this.diskServiceReleaser = diskServiceReleaser;
    }


    @Override
    public Optional<ResultSubpartition.BufferAndBacklog> getNextBuffer(int nextBufferToConsume)
            throws Throwable {
        if (!checkAndGetFirstBufferIndexOrError(nextBufferToConsume).isPresent()) {
            return Optional.empty();
        }

        // already ensure that peek element is not null and not throwable.
        BufferIndexOrError current = checkNotNull(loadedBuffers.poll());

        BufferIndexOrError next = loadedBuffers.peek();

        Buffer.DataType nextDataType = next == null ? Buffer.DataType.NONE : next.getDataType();
        int backlog = loadedBuffers.size();
        int bufferIndex = current.getIndex();
        Buffer buffer =
                current.getBuffer()
                        .orElseThrow(
                                () ->
                                        new NullPointerException(
                                                "Get a non-throwable and non-buffer bufferIndexOrError, which is not allowed"));
        return Optional.of(
                ResultSubpartition.BufferAndBacklog.fromBufferAndLookahead(
                        buffer, nextDataType, backlog, bufferIndex));
    }

    @Override
    public void release() {
        BufferIndexOrError bufferIndexOrError;
        while ((bufferIndexOrError = loadedBuffers.pollLast()) != null) {
            if (bufferIndexOrError.getBuffer().isPresent()) {
                checkNotNull(bufferIndexOrError.getBuffer().get()).recycleBuffer();
            }
        }
        diskServiceReleaser.run();
    }

    @Override
    public int getBacklog() {
        return loadedBuffers.size();
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    private Optional<BufferIndexOrError> checkAndGetFirstBufferIndexOrError(int expectedBufferIndex)
            throws Throwable {
        if (loadedBuffers.isEmpty()) {
            return Optional.empty();
        }

        BufferIndexOrError peek = loadedBuffers.peek();
        if (peek.getThrowable().isPresent()) {
            throw peek.getThrowable().get();
        }
        checkState(peek.getIndex() == expectedBufferIndex);
        return Optional.of(peek);
    }
}
