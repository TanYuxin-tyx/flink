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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.flink_31636;

import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.service.NettyBufferQueue;
import org.apache.flink.util.function.FunctionWithException;

import java.util.Optional;
import java.util.function.Supplier;

/**
 * The {@link TestingNettyBufferQueue} is used to mock the implementation of {@link
 * NettyBufferQueue}.
 */
public class TestingNettyBufferQueue implements NettyBufferQueue {

    public static final TestingNettyBufferQueue NO_OP = TestingNettyBufferQueue.builder().build();

    private final FunctionWithException<Integer, Optional<BufferAndBacklog>, Throwable>
            consumeBufferFunction;

    private final Supplier<Integer> getBacklogSupplier;

    private final Runnable releaseRunnable;

    private TestingNettyBufferQueue(
            FunctionWithException<Integer, Optional<BufferAndBacklog>, Throwable>
                    consumeBufferFunction,
            Supplier<Integer> getBacklogSupplier,
            Runnable releaseRunnable) {
        this.consumeBufferFunction = consumeBufferFunction;
        this.getBacklogSupplier = getBacklogSupplier;
        this.releaseRunnable = releaseRunnable;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public Optional<BufferAndBacklog> getNextBuffer(int bufferIndex) throws Throwable {
        return consumeBufferFunction.apply(bufferIndex);
    }

    @Override
    public int getBacklog() {
        return getBacklogSupplier.get();
    }

    @Override
    public void release() {
        releaseRunnable.run();
    }

    /** Builder for {@link TestingNettyBufferQueue}. */
    public static class Builder {

        private FunctionWithException<Integer, Optional<BufferAndBacklog>, Throwable>
                consumeBufferFunction = (ignore) -> Optional.empty();

        private Supplier<Integer> getBacklogSupplier = () -> 0;

        private Runnable releaseRunnable = () -> {};

        private Builder() {}

        public Builder setConsumeBufferFunction(
                FunctionWithException<Integer, Optional<BufferAndBacklog>, Throwable>
                        consumeBufferFunction) {
            this.consumeBufferFunction = consumeBufferFunction;
            return this;
        }

        public Builder setGetBacklogSupplier(Supplier<Integer> getBacklogSupplier) {
            this.getBacklogSupplier = getBacklogSupplier;
            return this;
        }

        public Builder setReleaseDataViewRunnable(Runnable releaseDataViewRunnable) {
            this.releaseRunnable = releaseDataViewRunnable;
            return this;
        }

        public TestingNettyBufferQueue build() {
            return new TestingNettyBufferQueue(
                    consumeBufferFunction, getBacklogSupplier, releaseRunnable);
        }
    }
}
