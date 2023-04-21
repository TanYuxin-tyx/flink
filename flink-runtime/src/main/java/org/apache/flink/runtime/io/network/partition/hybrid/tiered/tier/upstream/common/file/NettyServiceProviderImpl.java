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
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.common.BufferContext;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.service.NettyServiceProvider;

import java.util.Deque;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** The implementation of {@link NettyServiceProvider}. */
public class NettyServiceProviderImpl implements NettyServiceProvider {

    private final Deque<BufferContext> loadedBuffers;

    private final Runnable nettyServiceProviderReleaser;

    public NettyServiceProviderImpl(
            Deque<BufferContext> loadedBuffers, Runnable nettyServiceProviderReleaser) {
        this.loadedBuffers = loadedBuffers;
        this.nettyServiceProviderReleaser = nettyServiceProviderReleaser;
    }

    @Override
    public Optional<ResultSubpartition.BufferAndBacklog> getNextBuffer(int nextBufferToConsume)
            throws Throwable {
        if (!checkAndGetFirstBufferIndexOrError(nextBufferToConsume).isPresent()) {
            return Optional.empty();
        }
        BufferContext current = checkNotNull(loadedBuffers.poll());
        BufferContext next = loadedBuffers.peek();
        Buffer.DataType nextDataType =
                next == null ? Buffer.DataType.NONE : checkNotNull(next.getBuffer()).getDataType();
        int backlog = loadedBuffers.size();
        int bufferIndex = checkNotNull(current.getBufferIndexAndChannel()).getBufferIndex();
        return Optional.of(
                ResultSubpartition.BufferAndBacklog.fromBufferAndLookahead(
                        current.getBuffer(), nextDataType, backlog, bufferIndex));
    }

    @Override
    public void release() {
        BufferContext bufferContext;
        while ((bufferContext = loadedBuffers.pollLast()) != null) {
            if (bufferContext.getBuffer() != null) {
                checkNotNull(bufferContext.getBuffer()).recycleBuffer();
            }
        }
        nettyServiceProviderReleaser.run();
    }

    @Override
    public int getBacklog() {
        return loadedBuffers.size();
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    private Optional<BufferContext> checkAndGetFirstBufferIndexOrError(int expectedBufferIndex)
            throws Throwable {
        if (loadedBuffers.isEmpty()) {
            return Optional.empty();
        }

        BufferContext peek = loadedBuffers.peek();
        if (peek.getThrowable() != null) {
            throw peek.getThrowable();
        }
        checkState(
                checkNotNull(peek.getBufferIndexAndChannel()).getBufferIndex()
                        == expectedBufferIndex);
        return Optional.of(peek);
    }
}
