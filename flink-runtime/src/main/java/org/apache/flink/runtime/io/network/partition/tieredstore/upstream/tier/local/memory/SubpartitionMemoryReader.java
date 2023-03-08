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

package org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.memory;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.BufferContext;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierReader;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierReaderViewId;
import org.apache.flink.util.function.SupplierWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.util.Deque;
import java.util.LinkedList;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.locks.Lock;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class is responsible for managing the data of a single consumer. {@link
 * SubpartitionMemoryReader} will create a new {@link SubpartitionMemoryReader} when
 * a consumer is registered.
 */
public class SubpartitionMemoryReader implements TierReader {

    private static final Logger LOG =
            LoggerFactory.getLogger(SubpartitionMemoryReader.class);

    @GuardedBy("consumerLock")
    private final Deque<BufferContext> unConsumedBuffers = new LinkedList<>();

    private final Lock consumerLock;

    private final TierReaderViewId tierReaderViewId;

    private final int subpartitionId;

    private final MemoryDataWriterOperation memoryDataWriterOperation;

    public SubpartitionMemoryReader(
            Lock consumerLock,
            int subpartitionId,
            TierReaderViewId tierReaderViewId,
            MemoryDataWriterOperation memoryDataWriterOperation) {
        this.consumerLock = consumerLock;
        this.subpartitionId = subpartitionId;
        this.tierReaderViewId = tierReaderViewId;
        this.memoryDataWriterOperation = memoryDataWriterOperation;
    }

    @GuardedBy("consumerLock")
    // this method only called from subpartitionMemoryDataManager with write lock.
    public void addInitialBuffers(Deque<BufferContext> buffers) {
        unConsumedBuffers.addAll(buffers);
    }

    @GuardedBy("consumerLock")
    // this method only called from subpartitionMemoryDataManager with write lock.
    public boolean addBuffer(BufferContext bufferContext) {
        unConsumedBuffers.add(bufferContext);
        trimHeadingReleasedBuffers();
        return unConsumedBuffers.size() <= 1;
    }

    /**
     * Check whether the head of {@link #unConsumedBuffers} is the buffer to be consumed. If so,
     * return the buffer and backlog.
     *
     * @param toConsumeIndex index of buffer to be consumed.
     * @return If the head of {@link #unConsumedBuffers} is target, return optional of the buffer
     *     and backlog. Otherwise, return {@link Optional#empty()}.
     */
    @SuppressWarnings("FieldAccessNotGuarded")
    // Note that: callWithLock ensure that code block guarded by resultPartitionReadLock and
    // subpartitionLock.
    @Override
    public Optional<ResultSubpartition.BufferAndBacklog> consumeBuffer(
            int toConsumeIndex, Queue<Buffer> errorBuffers) {
        Optional<Tuple2<BufferContext, Buffer.DataType>> bufferAndNextDataType =
                callWithLock(
                        () -> {
                            if (!checkFirstUnConsumedBufferIndex(toConsumeIndex)) {
                                return Optional.empty();
                            }

                            BufferContext bufferContext =
                                    checkNotNull(unConsumedBuffers.pollFirst());
                            Buffer.DataType nextDataType =
                                    peekNextToConsumeDataTypeInternal(toConsumeIndex + 1);
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

    /**
     * Check whether the head of {@link #unConsumedBuffers} is the buffer to be consumed next time.
     * If so, return the next buffer's data type.
     *
     * @param nextToConsumeIndex index of the buffer to be consumed next time.
     * @return If the head of {@link #unConsumedBuffers} is target, return the buffer's data type.
     *     Otherwise, return {@link Buffer.DataType#NONE}.
     */
    @SuppressWarnings("FieldAccessNotGuarded")
    // Note that: callWithLock ensure that code block guarded by resultPartitionReadLock and
    // consumerLock.
    @Override
    public Buffer.DataType peekNextToConsumeDataType(
            int nextToConsumeIndex, Queue<Buffer> errorBuffers) {
        return callWithLock(() -> peekNextToConsumeDataTypeInternal(nextToConsumeIndex));
    }

    @GuardedBy("consumerLock")
    private Buffer.DataType peekNextToConsumeDataTypeInternal(int nextToConsumeIndex) {
        return checkFirstUnConsumedBufferIndex(nextToConsumeIndex)
                ? checkNotNull(unConsumedBuffers.peekFirst()).getBuffer().getDataType()
                : Buffer.DataType.NONE;
    }

    @GuardedBy("consumerLock")
    private boolean checkFirstUnConsumedBufferIndex(int expectedBufferIndex) {
        trimHeadingReleasedBuffers();
        return !unConsumedBuffers.isEmpty()
                && unConsumedBuffers.peekFirst().getBufferIndexAndChannel().getBufferIndex()
                        == expectedBufferIndex;
    }

    @SuppressWarnings("FieldAccessNotGuarded")
    // Un-synchronized get unConsumedBuffers size to provide memory data backlog,this will make the
    // result greater than or equal to the actual backlog, but obtaining an accurate backlog will
    // bring too much extra overhead.
    @Override
    public int getBacklog() {
        return unConsumedBuffers.size();
    }

    @Override
    public void releaseDataView() {
        memoryDataWriterOperation.onConsumerReleased(subpartitionId, tierReaderViewId);
    }

    @GuardedBy("consumerLock")
    private void trimHeadingReleasedBuffers() {
        while (!unConsumedBuffers.isEmpty() && unConsumedBuffers.peekFirst().isReleased()) {
            unConsumedBuffers.removeFirst();
        }
    }

    private <R, E extends Exception> R callWithLock(SupplierWithException<R, E> callable) throws E {
        try {
            consumerLock.lock();
            return callable.get();
        } finally {
            consumerLock.unlock();
        }
    }
}
