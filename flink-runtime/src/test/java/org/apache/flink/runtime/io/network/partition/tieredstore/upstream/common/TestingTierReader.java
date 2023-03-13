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
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.util.function.FunctionWithException;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

/** The {@link TestingTierReader} is used to mock the implementation of {@link TierReader}. */
public class TestingTierReader implements TierReader {
    public static final TestingTierReader NO_OP = TestingTierReader.builder().build();

    private final FunctionWithException<
                    Integer, Optional<ResultSubpartition.BufferAndBacklog>, Throwable>
            consumeBufferFunction;

    private final Supplier<Integer> getBacklogSupplier;

    private final Runnable releaseDataViewRunnable;

    private TestingTierReader(
            FunctionWithException<Integer, Optional<ResultSubpartition.BufferAndBacklog>, Throwable>
                    consumeBufferFunction,
            Supplier<Integer> getBacklogSupplier,
            Runnable releaseDataViewRunnable) {
        this.consumeBufferFunction = consumeBufferFunction;
        this.getBacklogSupplier = getBacklogSupplier;
        this.releaseDataViewRunnable = releaseDataViewRunnable;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public Optional<ResultSubpartition.BufferAndBacklog> consumeBuffer(int nextBufferToConsume)
            throws Throwable {
        return consumeBufferFunction.apply(nextBufferToConsume);
    }

    @Override
    public int getBacklog() {
        return getBacklogSupplier.get();
    }

    @Override
    public void release() {
        releaseDataViewRunnable.run();
    }

    /** Builder for {@link TestingTierReader}. */
    public static class Builder {
        private FunctionWithException<
                        Integer, Optional<ResultSubpartition.BufferAndBacklog>, Throwable>
                consumeBufferFunction = (ignore) -> Optional.empty();

        private Function<Integer, Buffer.DataType> peekNextToConsumeDataTypeFunction =
                (ignore) -> Buffer.DataType.NONE;

        private Supplier<Integer> getBacklogSupplier = () -> 0;

        private Runnable releaseDataViewRunnable = () -> {};

        private Builder() {}

        public Builder setConsumeBufferFunction(
                FunctionWithException<
                                Integer, Optional<ResultSubpartition.BufferAndBacklog>, Throwable>
                        consumeBufferFunction) {
            this.consumeBufferFunction = consumeBufferFunction;
            return this;
        }

        public Builder setPeekNextToConsumeDataTypeFunction(
                Function<Integer, Buffer.DataType> peekNextToConsumeDataTypeFunction) {
            this.peekNextToConsumeDataTypeFunction = peekNextToConsumeDataTypeFunction;
            return this;
        }

        public Builder setGetBacklogSupplier(Supplier<Integer> getBacklogSupplier) {
            this.getBacklogSupplier = getBacklogSupplier;
            return this;
        }

        public Builder setReleaseDataViewRunnable(Runnable releaseDataViewRunnable) {
            this.releaseDataViewRunnable = releaseDataViewRunnable;
            return this;
        }

        public TestingTierReader build() {
            return new TestingTierReader(consumeBufferFunction, getBacklogSupplier, releaseDataViewRunnable);
        }
    }
}
