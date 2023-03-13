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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.util.function.SupplierWithException;

import java.util.Deque;
import java.util.LinkedList;
import java.util.Optional;
import java.util.concurrent.locks.Lock;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** The {@link TierReaderImpl} is the default implementation of {@link TierReader}. */
public abstract class TierReaderImpl implements TierReader {

    protected final Deque<BufferContext> unConsumedBuffers = new LinkedList<>();

    private final Lock readerLock;

    public TierReaderImpl(Lock readerLock) {
        this.readerLock = readerLock;
    }

    public void addInitialBuffers(Deque<BufferContext> buffers) {
        unConsumedBuffers.addAll(buffers);
    }

    public boolean addBuffer(BufferContext bufferContext) {
        unConsumedBuffers.add(bufferContext);
        return unConsumedBuffers.size() <= 1;
    }

    @Override
    public Optional<ResultSubpartition.BufferAndBacklog> consumeBuffer(int toConsumeIndex) {
        Optional<Tuple2<BufferContext, Buffer.DataType>> bufferAndNextDataType =
                callWithLock(
                        () -> {
                            if (unConsumedBuffers.isEmpty()) {
                                return Optional.empty();
                            }
                            BufferContext bufferContext =
                                    checkNotNull(unConsumedBuffers.pollFirst());
                            checkState(
                                    bufferContext.getBufferIndexAndChannel().getBufferIndex()
                                            == toConsumeIndex);
                            Buffer.DataType nextDataType =
                                    unConsumedBuffers.isEmpty()
                                            ? Buffer.DataType.NONE
                                            : checkNotNull(unConsumedBuffers.peekFirst())
                                                    .getBuffer()
                                                    .getDataType();
                            return Optional.of(Tuple2.of(bufferContext, nextDataType));
                        });
        return bufferAndNextDataType.map(
                tuple ->
                        new ResultSubpartition.BufferAndBacklog(
                                tuple.f0.getBuffer().readOnlySlice(),
                                getBacklog(),
                                tuple.f1,
                                toConsumeIndex,
                                tuple.f0.isLastBufferInSegment()));
    }

    @Override
    public int getBacklog() {
        return unConsumedBuffers.size();
    }

    protected <R, E extends Exception> R callWithLock(SupplierWithException<R, E> callable)
            throws E {
        try {
            readerLock.lock();
            return callable.get();
        } finally {
            readerLock.unlock();
        }
    }
}
