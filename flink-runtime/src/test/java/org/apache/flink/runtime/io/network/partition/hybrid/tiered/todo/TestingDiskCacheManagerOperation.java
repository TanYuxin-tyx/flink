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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.todo;

import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.BufferContext;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.local.disk.DiskCacheManagerOperation;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyServiceWriterId;
import org.apache.flink.util.function.SupplierWithException;

import java.util.List;
import java.util.function.BiConsumer;

/** Mock {@link DiskCacheManagerOperation} for testing. */
public class TestingDiskCacheManagerOperation implements DiskCacheManagerOperation {
    private final SupplierWithException<BufferBuilder, InterruptedException>
            requestBufferFromPoolSupplier;

    private final BiConsumer<Integer, Integer> markBufferReadableConsumer;

    private final Runnable onDataAvailableRunnable;

    private final BiConsumer<Integer, NettyServiceWriterId> onConsumerReleasedBiConsumer;

    private TestingDiskCacheManagerOperation(
            SupplierWithException<BufferBuilder, InterruptedException>
                    requestBufferFromPoolSupplier,
            BiConsumer<Integer, Integer> markBufferReadableConsumer,
            Runnable onDataAvailableRunnable,
            BiConsumer<Integer, NettyServiceWriterId> onConsumerReleasedBiConsumer) {
        this.requestBufferFromPoolSupplier = requestBufferFromPoolSupplier;
        this.markBufferReadableConsumer = markBufferReadableConsumer;
        this.onDataAvailableRunnable = onDataAvailableRunnable;
        this.onConsumerReleasedBiConsumer = onConsumerReleasedBiConsumer;
    }

    @Override
    public int getNumSubpartitions() {
        return 0;
    }

    @Override
    public List<BufferContext> getBuffersInOrder(int subpartitionId) {
        return null;
    }

    @Override
    public void onConsumerReleased(
            int subpartitionId, NettyServiceWriterId nettyServiceWriterId) {
        onConsumerReleasedBiConsumer.accept(subpartitionId, nettyServiceWriterId);
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder for {@link TestingDiskCacheManagerOperation}. */
    public static class Builder {
        private SupplierWithException<BufferBuilder, InterruptedException>
                requestBufferFromPoolSupplier = () -> null;

        private BiConsumer<Integer, Integer> markBufferReadableConsumer = (ignore1, ignore2) -> {};

        private Runnable onDataAvailableRunnable = () -> {};

        private BiConsumer<Integer, NettyServiceWriterId> onConsumerReleasedBiConsumer =
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

        public Builder setOnDataAvailableRunnable(Runnable onDataAvailableRunnable) {
            this.onDataAvailableRunnable = onDataAvailableRunnable;
            return this;
        }

        public Builder setOnConsumerReleasedBiConsumer(
                BiConsumer<Integer, NettyServiceWriterId> onConsumerReleasedBiConsumer) {
            this.onConsumerReleasedBiConsumer = onConsumerReleasedBiConsumer;
            return this;
        }

        private Builder() {}

        public TestingDiskCacheManagerOperation build() {
            return new TestingDiskCacheManagerOperation(
                    requestBufferFromPoolSupplier,
                    markBufferReadableConsumer,
                    onDataAvailableRunnable,
                    onConsumerReleasedBiConsumer);
        }
    }
}
