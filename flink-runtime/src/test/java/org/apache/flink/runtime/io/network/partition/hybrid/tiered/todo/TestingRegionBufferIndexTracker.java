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

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file.FileReaderId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.disk.RegionBufferIndexTracker;
import org.apache.flink.util.function.TriFunction;

import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/** Mock {@link RegionBufferIndexTracker} for testing. */
public class TestingRegionBufferIndexTracker implements RegionBufferIndexTracker {
    private final TriFunction<Integer, Integer, Integer, Optional<ReadableRegion>>
            getReadableRegionFunction;

    private final Consumer<List<SpilledBuffer>> addBuffersConsumer;

    private final BiConsumer<Integer, Integer> markBufferReadableConsumer;

    private TestingRegionBufferIndexTracker(
            TriFunction<Integer, Integer, Integer, Optional<ReadableRegion>>
                    getReadableRegionFunction,
            Consumer<List<SpilledBuffer>> addBuffersConsumer,
            BiConsumer<Integer, Integer> markBufferReadableConsumer) {
        this.getReadableRegionFunction = getReadableRegionFunction;
        this.addBuffersConsumer = addBuffersConsumer;
        this.markBufferReadableConsumer = markBufferReadableConsumer;
    }

    @Override
    public Optional<ReadableRegion> getReadableRegion(
            int subpartitionId, int bufferIndex, FileReaderId nettyServiceWriterId) {
        return getReadableRegionFunction.apply(subpartitionId, bufferIndex, 0);
    }

    @Override
    public void addBuffers(List<SpilledBuffer> spilledBuffers) {
        addBuffersConsumer.accept(spilledBuffers);
    }

    @Override
    public void release() {}

    public static Builder builder() {
        return new Builder();
    }

    /** Builder for {@link TestingRegionBufferIndexTracker}. */
    public static class Builder {
        private TriFunction<Integer, Integer, Integer, Optional<ReadableRegion>>
                getReadableRegionFunction = (ignore1, ignore2, ignore3) -> Optional.empty();

        private Consumer<List<SpilledBuffer>> addBuffersConsumer = (ignore) -> {};

        private BiConsumer<Integer, Integer> markBufferReadableConsumer = (ignore1, ignore2) -> {};

        private Builder() {}

        public Builder setGetReadableRegionFunction(
                TriFunction<Integer, Integer, Integer, Optional<ReadableRegion>>
                        getReadableRegionFunction) {
            this.getReadableRegionFunction = getReadableRegionFunction;
            return this;
        }

        public Builder setAddBuffersConsumer(Consumer<List<SpilledBuffer>> addBuffersConsumer) {
            this.addBuffersConsumer = addBuffersConsumer;
            return this;
        }

        public Builder setMarkBufferReadableConsumer(
                BiConsumer<Integer, Integer> markBufferReadableConsumer) {
            this.markBufferReadableConsumer = markBufferReadableConsumer;
            return this;
        }

        public TestingRegionBufferIndexTracker build() {
            return new TestingRegionBufferIndexTracker(
                    getReadableRegionFunction, addBuffersConsumer, markBufferReadableConsumer);
        }
    }
}
