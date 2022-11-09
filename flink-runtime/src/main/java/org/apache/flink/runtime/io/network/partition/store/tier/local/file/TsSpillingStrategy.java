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

package org.apache.flink.runtime.io.network.partition.store.tier.local.file;

import org.apache.flink.runtime.io.network.partition.store.common.BufferIndexAndChannel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Spilling strategy for tiered store shuffle mode. */
public interface TsSpillingStrategy {

    /**
     * Make a decision when a buffer becomes finished.
     *
     * @param numTotalUnSpillBuffers total number of buffers not spill.
     * @return A {@link Decision} based on the provided information, or {@link Optional#empty()} if
     *     the decision cannot be made, which indicates global information is needed.
     */
    Optional<Decision> onBufferFinished(int numTotalUnSpillBuffers, int currentPoolSize);

    /**
     * Make a decision when a buffer is consumed.
     *
     * @param consumedBuffer the buffer that is consumed.
     * @return A {@link Decision} based on the provided information, or {@link Optional#empty()} if
     *     the decision cannot be made, which indicates global information is needed.
     */
    Optional<Decision> onBufferConsumed(BufferIndexAndChannel consumedBuffer);

    Decision forceTriggerFlushCachedBuffers(BufferSpillingInfoProvider spillingInfoProvider);

    /**
     * Make a decision when result partition is closed. Because this method will directly touch the
     * {@link BufferSpillingInfoProvider}, the caller should take care of the thread safety.
     *
     * @param spillingInfoProvider that provides information about the current status.
     * @return A {@link Decision} based on the global information.
     */
    Decision onResultPartitionClosed(BufferSpillingInfoProvider spillingInfoProvider);

    /**
     * This class represents the spill and release decision made by {@link TsSpillingStrategy}, in
     * other words, which data is to be spilled and which data is to be released.
     */
    class Decision {
        /** A collection of buffer that needs to be spilled to disk. */
        private final Map<Integer, List<BufferIndexAndChannel>> bufferToSpill;

        /** A collection of buffer that needs to be released. */
        private final Map<Integer, List<BufferIndexAndChannel>> bufferToRelease;

        public static final Decision NO_ACTION =
                new Decision(Collections.emptyMap(), Collections.emptyMap());

        private Decision(
                Map<Integer, List<BufferIndexAndChannel>> bufferToSpill,
                Map<Integer, List<BufferIndexAndChannel>> bufferToRelease) {
            this.bufferToSpill = bufferToSpill;
            this.bufferToRelease = bufferToRelease;
        }

        public Map<Integer, List<BufferIndexAndChannel>> getBufferToSpill() {
            return bufferToSpill;
        }

        public Map<Integer, List<BufferIndexAndChannel>> getBufferToRelease() {
            return bufferToRelease;
        }

        public static Builder builder() {
            return new Builder();
        }

        /** Builder for {@link Decision}. */
        public static class Builder {
            /** A collection of buffer that needs to be spilled to disk. */
            private final Map<Integer, List<BufferIndexAndChannel>> bufferToSpill = new HashMap<>();

            /** A collection of buffer that needs to be released. */
            private final Map<Integer, List<BufferIndexAndChannel>> bufferToRelease =
                    new HashMap<>();

            private Builder() {}

            public Builder addBufferToSpill(
                    int subpartitionId, List<BufferIndexAndChannel> buffers) {
                bufferToSpill.computeIfAbsent(subpartitionId, ArrayList::new).addAll(buffers);
                return this;
            }

            public Builder addBufferToSpill(
                    int subpartitionId, Deque<BufferIndexAndChannel> buffers) {
                bufferToSpill.computeIfAbsent(subpartitionId, ArrayList::new).addAll(buffers);
                return this;
            }

            public Builder addBufferToRelease(BufferIndexAndChannel buffer) {
                bufferToRelease.computeIfAbsent(buffer.getChannel(), ArrayList::new).add(buffer);
                return this;
            }

            public Builder addBufferToRelease(
                    int subpartitionId, List<BufferIndexAndChannel> buffers) {
                bufferToRelease.computeIfAbsent(subpartitionId, ArrayList::new).addAll(buffers);
                return this;
            }

            public Builder addBufferToRelease(
                    int subpartitionId, Deque<BufferIndexAndChannel> buffers) {
                bufferToRelease.computeIfAbsent(subpartitionId, ArrayList::new).addAll(buffers);
                return this;
            }

            public Decision build() {
                return new Decision(bufferToSpill, bufferToRelease);
            }
        }
    }
}
