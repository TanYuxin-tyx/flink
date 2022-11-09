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

package org.apache.flink.runtime.io.network.partition.store.local.memory;

import org.apache.flink.runtime.io.network.partition.store.common.BufferIndexAndChannel;
import org.apache.flink.runtime.io.network.partition.store.tier.local.file.BufferSpillingInfoProvider;
import org.apache.flink.runtime.io.network.partition.store.tier.local.file.TsSpillingStrategy;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

/** Mock {@link TsSpillingStrategy} for testing. */
public class TestingSpillingStrategy implements TsSpillingStrategy {
    private final BiFunction<Integer, Integer, Optional<Decision>> onMemoryUsageChangedFunction;

    private final BiFunction<Integer, Integer, Optional<Decision>> onBufferFinishedFunction;

    private final Function<BufferIndexAndChannel, Optional<Decision>> onBufferConsumedFunction;

    private final Function<BufferSpillingInfoProvider, Decision> decideActionWithGlobalInfoFunction;

    private final Function<BufferSpillingInfoProvider, Decision> forceFlushCachedBuffersFunction;

    private final Function<BufferSpillingInfoProvider, Decision> onResultPartitionClosedFunction;

    private TestingSpillingStrategy(
            BiFunction<Integer, Integer, Optional<Decision>> onMemoryUsageChangedFunction,
            BiFunction<Integer, Integer, Optional<Decision>> onBufferFinishedFunction,
            Function<BufferIndexAndChannel, Optional<Decision>> onBufferConsumedFunction,
            Function<BufferSpillingInfoProvider, Decision> decideActionWithGlobalInfoFunction,
            Function<BufferSpillingInfoProvider, Decision> forceFlushCachedBuffersFunction,
            Function<BufferSpillingInfoProvider, Decision> onResultPartitionClosedFunction) {
        this.onMemoryUsageChangedFunction = onMemoryUsageChangedFunction;
        this.onBufferFinishedFunction = onBufferFinishedFunction;
        this.onBufferConsumedFunction = onBufferConsumedFunction;
        this.decideActionWithGlobalInfoFunction = decideActionWithGlobalInfoFunction;
        this.forceFlushCachedBuffersFunction = forceFlushCachedBuffersFunction;
        this.onResultPartitionClosedFunction = onResultPartitionClosedFunction;
    }

    @Override
    public Optional<Decision> onBufferFinished(int numTotalUnSpillBuffers, int currentPoolSize) {
        return onBufferFinishedFunction.apply(numTotalUnSpillBuffers, currentPoolSize);
    }

    @Override
    public Optional<Decision> onBufferConsumed(BufferIndexAndChannel consumedBuffer) {
        return onBufferConsumedFunction.apply(consumedBuffer);
    }

    @Override
    public Decision forceTriggerFlushCachedBuffers(
            BufferSpillingInfoProvider spillingInfoProvider) {
        return forceFlushCachedBuffersFunction.apply(spillingInfoProvider);
    }

    @Override
    public Decision onResultPartitionClosed(BufferSpillingInfoProvider spillingInfoProvider) {
        return onResultPartitionClosedFunction.apply(spillingInfoProvider);
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder for {@link TestingSpillingStrategy}. */
    public static class Builder {
        private BiFunction<Integer, Integer, Optional<Decision>> onMemoryUsageChangedFunction =
                (ignore1, ignore2) -> Optional.of(Decision.NO_ACTION);

        private BiFunction<Integer, Integer, Optional<Decision>> onBufferFinishedFunction =
                (ignore1, ignore2) -> Optional.of(Decision.NO_ACTION);

        private Function<BufferIndexAndChannel, Optional<Decision>> onBufferConsumedFunction =
                (ignore) -> Optional.of(Decision.NO_ACTION);

        private Function<BufferSpillingInfoProvider, Decision> decideActionWithGlobalInfoFunction =
                (ignore) -> Decision.NO_ACTION;

        private Function<BufferSpillingInfoProvider, Decision> forceFlushCachedBuffersFunction =
                (ignore) -> Decision.NO_ACTION;

        private Function<BufferSpillingInfoProvider, Decision> onResultPartitionClosedFunction =
                (ignore) -> Decision.NO_ACTION;

        private Builder() {}

        public Builder setOnMemoryUsageChangedFunction(
                BiFunction<Integer, Integer, Optional<Decision>> onMemoryUsageChangedFunction) {
            this.onMemoryUsageChangedFunction = onMemoryUsageChangedFunction;
            return this;
        }

        public Builder setOnBufferFinishedFunction(
                BiFunction<Integer, Integer, Optional<Decision>> onBufferFinishedFunction) {
            this.onBufferFinishedFunction = onBufferFinishedFunction;
            return this;
        }

        public Builder setOnBufferConsumedFunction(
                Function<BufferIndexAndChannel, Optional<Decision>> onBufferConsumedFunction) {
            this.onBufferConsumedFunction = onBufferConsumedFunction;
            return this;
        }

        public Builder setDecideActionWithGlobalInfoFunction(
                Function<BufferSpillingInfoProvider, Decision> decideActionWithGlobalInfoFunction) {
            this.decideActionWithGlobalInfoFunction = decideActionWithGlobalInfoFunction;
            return this;
        }

        public Builder setForceFlushCachedBuffersFunction(
                Function<BufferSpillingInfoProvider, Decision> forceFlushCachedBuffersFunction) {
            this.forceFlushCachedBuffersFunction = forceFlushCachedBuffersFunction;
            return this;
        }

        public Builder setOnResultPartitionClosedFunction(
                Function<BufferSpillingInfoProvider, Decision> onResultPartitionClosedFunction) {
            this.onResultPartitionClosedFunction = onResultPartitionClosedFunction;
            return this;
        }

        public TestingSpillingStrategy build() {
            return new TestingSpillingStrategy(
                    onMemoryUsageChangedFunction,
                    onBufferFinishedFunction,
                    onBufferConsumedFunction,
                    decideActionWithGlobalInfoFunction,
                    forceFlushCachedBuffersFunction,
                    onResultPartitionClosedFunction);
        }
    }
}
