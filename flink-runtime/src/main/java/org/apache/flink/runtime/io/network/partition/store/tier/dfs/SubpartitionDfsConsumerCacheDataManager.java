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

package org.apache.flink.runtime.io.network.partition.store.tier.dfs;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.runtime.io.network.partition.store.common.BufferContext;
import org.apache.flink.runtime.io.network.partition.store.common.ConsumerId;
import org.apache.flink.runtime.io.network.partition.store.common.BufferConsumeView;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.concurrent.GuardedBy;

import java.util.Deque;
import java.util.LinkedList;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.locks.Lock;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class is responsible for managing the data of a single consumer. {@link
 * SubpartitionDfsCacheDataManager} will create a new {@link
 * SubpartitionDfsConsumerCacheDataManager} when a consumer is registered.
 */
public class SubpartitionDfsConsumerCacheDataManager implements BufferConsumeView {

    @GuardedBy("consumerLock")
    private final Deque<BufferContext> unConsumedBuffers = new LinkedList<>();

    private final Lock consumerLock;

    private final Lock resultPartitionLock;

    private final ConsumerId consumerId;

    private final int subpartitionId;

    private final DfsCacheDataManagerOperation cacheDataManagerOperation;

    public SubpartitionDfsConsumerCacheDataManager(
            Lock resultPartitionLock,
            Lock consumerLock,
            int subpartitionId,
            ConsumerId consumerId,
            DfsCacheDataManagerOperation cacheDataManagerOperation) {
        this.resultPartitionLock = resultPartitionLock;
        this.consumerLock = consumerLock;
        this.subpartitionId = subpartitionId;
        this.consumerId = consumerId;
        this.cacheDataManagerOperation = cacheDataManagerOperation;
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
                            Buffer.DataType nextDataType = Buffer.DataType.DATA_BUFFER;
                            return Optional.of(Tuple2.of(bufferContext, nextDataType));
                        });
        return bufferAndNextDataType.map(
                tuple -> {
                    ResultSubpartition.BufferAndBacklog bufferAndBacklog =
                            new ResultSubpartition.BufferAndBacklog(
                                    null, getBacklog(), tuple.f1, toConsumeIndex, true);
                    bufferAndBacklog.setFromDfsTier(true);
                    return bufferAndBacklog;
                });
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
        cacheDataManagerOperation.onConsumerReleased(subpartitionId, consumerId);
    }

    @GuardedBy("consumerLock")
    private void trimHeadingReleasedBuffers() {
        while (!unConsumedBuffers.isEmpty() && unConsumedBuffers.peekFirst().isReleased()) {
            unConsumedBuffers.removeFirst();
        }
    }

    private <R, E extends Exception> R callWithLock(SupplierWithException<R, E> callable) throws E {
        try {
            resultPartitionLock.lock();
            consumerLock.lock();
            return callable.get();
        } finally {
            consumerLock.unlock();
            resultPartitionLock.unlock();
        }
    }
}
