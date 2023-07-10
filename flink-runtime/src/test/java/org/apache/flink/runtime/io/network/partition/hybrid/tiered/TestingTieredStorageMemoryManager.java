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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemorySpec;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/** Test implementation for {@link TieredStorageMemoryManager}. */
public class TestingTieredStorageMemoryManager implements TieredStorageMemoryManager {

    private final BiConsumer<BufferPool, List<TieredStorageMemorySpec>> setupConsumer;

    private final Consumer<Runnable> listenBufferReclaimRequestConsumer;

    private final Function<Object, BufferBuilder> requestBufferBlockingFunction;

    private final Function<Object, Integer> getMaxNonReclaimableBuffersFunction;

    private final Function<Object, Integer> numOwnerRequestedBufferFunction;

    private final BiConsumer<Object, Object> transferBufferOwnershipConsumer;

    private final Function<Object, BufferRecycler> getOwnerBufferRecyclerFunction;

    private final Runnable releaseRunnable;

    private TestingTieredStorageMemoryManager(
            BiConsumer<BufferPool, List<TieredStorageMemorySpec>> setupConsumer,
            Consumer<Runnable> listenBufferReclaimRequestConsumer,
            Function<Object, BufferBuilder> requestBufferBlockingFunction,
            Function<Object, Integer> getMaxNonReclaimableBuffersFunction,
            Function<Object, Integer> numOwnerRequestedBufferFunction,
            BiConsumer<Object, Object> transferBufferOwnershipConsumer,
            Function<Object, BufferRecycler> getOwnerBufferRecyclerFunction,
            Runnable releaseRunnable) {
        this.setupConsumer = setupConsumer;
        this.listenBufferReclaimRequestConsumer = listenBufferReclaimRequestConsumer;
        this.requestBufferBlockingFunction = requestBufferBlockingFunction;
        this.getMaxNonReclaimableBuffersFunction = getMaxNonReclaimableBuffersFunction;
        this.numOwnerRequestedBufferFunction = numOwnerRequestedBufferFunction;
        this.transferBufferOwnershipConsumer = transferBufferOwnershipConsumer;
        this.getOwnerBufferRecyclerFunction = getOwnerBufferRecyclerFunction;
        this.releaseRunnable = releaseRunnable;
    }

    @Override
    public void setup(BufferPool bufferPool, List<TieredStorageMemorySpec> storageMemorySpecs) {
        setupConsumer.accept(bufferPool, storageMemorySpecs);
    }

    @Override
    public void listenBufferReclaimRequest(Runnable onBufferReclaimRequest) {
        listenBufferReclaimRequestConsumer.accept(onBufferReclaimRequest);
    }

    @Override
    public BufferBuilder requestBufferBlocking(Object owner) {
        return requestBufferBlockingFunction.apply(owner);
    }

    @Override
    public int getMaxNonReclaimableBuffers(Object owner) {
        return getMaxNonReclaimableBuffersFunction.apply(owner);
    }

    @Override
    public int numOwnerRequestedBuffer(Object owner) {
        return numOwnerRequestedBufferFunction.apply(owner);
    }

    @Override
    public void transferBufferOwnership(Object oldOwner, Object newOwner, Buffer buffer) {
        transferBufferOwnershipConsumer.accept(oldOwner, newOwner);
    }

    @Override
    public void release() {
        releaseRunnable.run();
    }

    /** Builder for {@link TestingTieredStorageMemoryManager}. */
    public static class Builder {

        private BiConsumer<BufferPool, List<TieredStorageMemorySpec>> setupConsumer =
                (bufferPool, tieredStorageMemorySpecs) -> {};

        private Consumer<Runnable> listenBufferReclaimRequestConsumer = runnable -> {};

        private Function<Object, BufferBuilder> requestBufferBlockingFunction = owner -> null;

        private Function<Object, Integer> getMaxNonReclaimableBuffersFunction = owner -> 0;

        private Function<Object, Integer> numOwnerRequestedBufferFunction = owner -> 0;

        private BiConsumer<Object, Object> transferBufferOwnershipConsumer =
                (oldOwner, newOwner) -> {};

        private Function<Object, BufferRecycler> getOwnerBufferRecyclerFunction = owner -> null;

        private Runnable releaseRunnable = () -> {};

        public Builder() {}

        public TestingTieredStorageMemoryManager.Builder setSetupConsumer(
                BiConsumer<BufferPool, List<TieredStorageMemorySpec>> setupConsumer) {
            this.setupConsumer = setupConsumer;
            return this;
        }

        public TestingTieredStorageMemoryManager.Builder setListenBufferReclaimRequestConsumer(
                Consumer<Runnable> listenBufferReclaimRequestConsumer) {
            this.listenBufferReclaimRequestConsumer = listenBufferReclaimRequestConsumer;
            return this;
        }

        public TestingTieredStorageMemoryManager.Builder setRequestBufferBlockingFunction(
                Function<Object, BufferBuilder> requestBufferBlockingFunction) {
            this.requestBufferBlockingFunction = requestBufferBlockingFunction;
            return this;
        }

        public TestingTieredStorageMemoryManager.Builder setGetMaxNonReclaimableBuffersFunction(
                Function<Object, Integer> getMaxNonReclaimableBuffersFunction) {
            this.getMaxNonReclaimableBuffersFunction = getMaxNonReclaimableBuffersFunction;
            return this;
        }

        public TestingTieredStorageMemoryManager.Builder setNumOwnerRequestedBufferFunction(
                Function<Object, Integer> numOwnerRequestedBufferFunction) {
            this.numOwnerRequestedBufferFunction = numOwnerRequestedBufferFunction;
            return this;
        }

        public TestingTieredStorageMemoryManager.Builder setTransferBufferOwnershipConsumer(
                BiConsumer<Object, Object> transferBufferOwnershipConsumer) {
            this.transferBufferOwnershipConsumer = transferBufferOwnershipConsumer;
            return this;
        }

        public TestingTieredStorageMemoryManager.Builder setGetOwnerBufferRecyclerFunction(
                Function<Object, BufferRecycler> getOwnerBufferRecyclerFunction) {
            this.getOwnerBufferRecyclerFunction = getOwnerBufferRecyclerFunction;
            return this;
        }

        public TestingTieredStorageMemoryManager.Builder setReleaseRunnable(
                Runnable releaseRunnable) {
            this.releaseRunnable = releaseRunnable;
            return this;
        }

        public TestingTieredStorageMemoryManager build() {
            return new TestingTieredStorageMemoryManager(
                    setupConsumer,
                    listenBufferReclaimRequestConsumer,
                    requestBufferBlockingFunction,
                    getMaxNonReclaimableBuffersFunction,
                    numOwnerRequestedBufferFunction,
                    transferBufferOwnershipConsumer,
                    getOwnerBufferRecyclerFunction,
                    releaseRunnable);
        }
    }
}
