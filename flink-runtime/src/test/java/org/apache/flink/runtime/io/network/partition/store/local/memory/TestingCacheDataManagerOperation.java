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

import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.partition.store.common.BufferIndexAndChannel;
import org.apache.flink.runtime.io.network.partition.store.common.ConsumerId;
import org.apache.flink.runtime.io.network.partition.store.tier.local.file.CacheDataManagerOperation;
import org.apache.flink.util.function.SupplierWithException;

import java.util.Collection;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/** Mock {@link CacheDataManagerOperation} for testing. */
public class TestingCacheDataManagerOperation implements CacheDataManagerOperation {
    private final SupplierWithException<BufferBuilder, InterruptedException>
            requestBufferFromPoolSupplier;

    private final BiConsumer<Integer, Integer> markBufferReadableConsumer;

    private final Consumer<BufferIndexAndChannel> onBufferConsumedConsumer;

    private final Runnable onBufferFinishedRunnable;

    private final Runnable onDataAvailableRunnable;

    private final BiConsumer<Integer, ConsumerId> onConsumerReleasedBiConsumer;

    private TestingCacheDataManagerOperation(
            SupplierWithException<BufferBuilder, InterruptedException>
                    requestBufferFromPoolSupplier,
            BiConsumer<Integer, Integer> markBufferReadableConsumer,
            Consumer<BufferIndexAndChannel> onBufferConsumedConsumer,
            Runnable onBufferFinishedRunnable,
            Runnable onDataAvailableRunnable,
            BiConsumer<Integer, ConsumerId> onConsumerReleasedBiConsumer) {
        this.requestBufferFromPoolSupplier = requestBufferFromPoolSupplier;
        this.markBufferReadableConsumer = markBufferReadableConsumer;
        this.onBufferConsumedConsumer = onBufferConsumedConsumer;
        this.onBufferFinishedRunnable = onBufferFinishedRunnable;
        this.onDataAvailableRunnable = onDataAvailableRunnable;
        this.onConsumerReleasedBiConsumer = onConsumerReleasedBiConsumer;
    }

    @Override
    public BufferBuilder requestBufferFromPool() throws InterruptedException {
        return requestBufferFromPoolSupplier.get();
    }

    @Override
    public void markBufferReleasedFromFile(int subpartitionId, int bufferIndex) {
        markBufferReadableConsumer.accept(subpartitionId, bufferIndex);
    }

    @Override
    public void onBufferConsumed(BufferIndexAndChannel consumedBuffer) {
        onBufferConsumedConsumer.accept(consumedBuffer);
    }

    @Override
    public void onBufferFinished() {
        onBufferFinishedRunnable.run();
    }

    @Override
    public void onDataAvailable(int subpartitionId, Collection<ConsumerId> consumerIds) {
        onDataAvailableRunnable.run();
    }

    @Override
    public void onConsumerReleased(int subpartitionId, ConsumerId consumerId) {
        onConsumerReleasedBiConsumer.accept(subpartitionId, consumerId);
    }

    @Override
    public boolean isLastBufferInSegment(int subpartitionId, int bufferIndex) {
        return false;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder for {@link TestingCacheDataManagerOperation}. */
    public static class Builder {
        private SupplierWithException<BufferBuilder, InterruptedException>
                requestBufferFromPoolSupplier = () -> null;

        private BiConsumer<Integer, Integer> markBufferReadableConsumer = (ignore1, ignore2) -> {};

        private Consumer<BufferIndexAndChannel> onBufferConsumedConsumer = (ignore1) -> {};

        private Runnable onBufferFinishedRunnable = () -> {};

        private Runnable onDataAvailableRunnable = () -> {};

        private BiConsumer<Integer, ConsumerId> onConsumerReleasedBiConsumer =
                (ignore1, ignore2) -> {};

        public Builder setRequestBufferFromPoolSupplier(
                SupplierWithException<BufferBuilder, InterruptedException>
                        requestBufferFromPoolSupplier) {
            this.requestBufferFromPoolSupplier = requestBufferFromPoolSupplier;
            return this;
        }

        public Builder setMarkBufferReadableConsumer(
                BiConsumer<Integer, Integer> markBufferReadableConsumer) {
            this.markBufferReadableConsumer = markBufferReadableConsumer;
            return this;
        }

        public Builder setOnBufferConsumedConsumer(
                Consumer<BufferIndexAndChannel> onBufferConsumedConsumer) {
            this.onBufferConsumedConsumer = onBufferConsumedConsumer;
            return this;
        }

        public Builder setOnBufferFinishedRunnable(Runnable onBufferFinishedRunnable) {
            this.onBufferFinishedRunnable = onBufferFinishedRunnable;
            return this;
        }

        public Builder setOnDataAvailableRunnable(Runnable onDataAvailableRunnable) {
            this.onDataAvailableRunnable = onDataAvailableRunnable;
            return this;
        }

        public Builder setOnConsumerReleasedBiConsumer(
                BiConsumer<Integer, ConsumerId> onConsumerReleasedBiConsumer) {
            this.onConsumerReleasedBiConsumer = onConsumerReleasedBiConsumer;
            return this;
        }

        private Builder() {}

        public TestingCacheDataManagerOperation build() {
            return new TestingCacheDataManagerOperation(
                    requestBufferFromPoolSupplier,
                    markBufferReadableConsumer,
                    onBufferConsumedConsumer,
                    onBufferFinishedRunnable,
                    onDataAvailableRunnable,
                    onConsumerReleasedBiConsumer);
        }
    }
}
