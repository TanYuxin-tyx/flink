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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.common;

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.disk.DiskTierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.memory.MemoryTierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.remote.RemoteTierFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/** Configuration for tiered storage. */
public class TieredStorageConfiguration {

    private static final String DEFAULT_REMOTE_STORAGE_BASE_PATH = null;

    private static final int DEFAULT_TIERED_STORAGE_BUFFER_SIZE = 32 * 1024;

    private static final int DEFAULT_MEMORY_TIER_EXCLUSIVE_BUFFERS = 100;

    private static final int DEFAULT_DISK_TIER_EXCLUSIVE_BUFFERS = 1;

    private static final int DEFAULT_REMOTE_TIER_EXCLUSIVE_BUFFERS = 1;

    private static final int DEFAULT_NUM_BUFFERS_USE_SORT_ACCUMULATOR_THRESHOLD = 512;

    private static final int DEFAULT_MEMORY_TIER_NUM_BYTES_PER_SEGMENT = 320 * 1024;

    private static final int DEFAULT_DISK_TIER_NUM_BYTES_PER_SEGMENT = 8 * 1024 * 1024;

    private static final int DEFAULT_REMOTE_TIER_NUM_BYTES_PER_SEGMENT = 8 * 1024 * 1024;

    private static final float DEFAULT_NUM_BUFFERS_TRIGGER_FLUSH_RATIO = 0.5f;

    private static final int DEFAULT_DISK_TIER_MAX_BUFFERS_READ_AHEAD = 5;

    private static final Duration DEFAULT_DISK_TIER_BUFFER_REQUEST_TIMEOUT = Duration.ofMinutes(5);

    private static final int DEFAULT_DISK_TIER_MAX_REQUEST_BUFFERS = 1000;

    private static final float DEFAULT_MIN_RESERVE_SPACE_FRACTION = 0.05f;

    private static final boolean DEFAULT_IS_HYBRID_SELECTIVE = true;

    private final String remoteStorageBasePath;

    private final int tieredStorageBufferSize;

    private final int memoryTierExclusiveBuffers;

    private final int diskTierExclusiveBuffers;

    private final int remoteTierExclusiveBuffers;

    private final int numBuffersUseSortAccumulatorThreshold;

    private final int memoryTierNumBytesPerSegment;

    private final int diskTierNumBytesPerSegment;

    private final int remoteTierNumBytesPerSegment;

    private final float numBuffersTriggerFlushRatio;

    private final int diskTierMaxBuffersReadAhead;

    private final Duration diskTierBufferRequestTimeout;

    private final int diskTierMaxRequestBuffers;

    private final float minReserveSpaceFraction;

    private final List<TierFactory> tierFactories;

    public TieredStorageConfiguration(
            String remoteStorageBasePath,
            int tieredStorageBufferSize,
            int memoryTierExclusiveBuffers,
            int diskTierExclusiveBuffers,
            int remoteTierExclusiveBuffers,
            int numBuffersUseSortAccumulatorThreshold,
            int memoryTierNumBytesPerSegment,
            int diskTierNumBytesPerSegment,
            int remoteTierNumBytesPerSegment,
            float numBuffersTriggerFlushRatio,
            int diskTierMaxBuffersReadAhead,
            Duration diskTierBufferRequestTimeout,
            int diskTierMaxRequestBuffers,
            float minReserveSpaceFraction,
            List<TierFactory> tierFactories) {
        this.remoteStorageBasePath = remoteStorageBasePath;
        this.tieredStorageBufferSize = tieredStorageBufferSize;
        this.memoryTierExclusiveBuffers = memoryTierExclusiveBuffers;
        this.diskTierExclusiveBuffers = diskTierExclusiveBuffers;
        this.remoteTierExclusiveBuffers = remoteTierExclusiveBuffers;
        this.numBuffersUseSortAccumulatorThreshold = numBuffersUseSortAccumulatorThreshold;
        this.memoryTierNumBytesPerSegment = memoryTierNumBytesPerSegment;
        this.diskTierNumBytesPerSegment = diskTierNumBytesPerSegment;
        this.remoteTierNumBytesPerSegment = remoteTierNumBytesPerSegment;
        this.numBuffersTriggerFlushRatio = numBuffersTriggerFlushRatio;
        this.diskTierMaxBuffersReadAhead = diskTierMaxBuffersReadAhead;
        this.diskTierBufferRequestTimeout = diskTierBufferRequestTimeout;
        this.diskTierMaxRequestBuffers = diskTierMaxRequestBuffers;
        this.minReserveSpaceFraction = minReserveSpaceFraction;
        this.tierFactories = tierFactories;
    }

    public static Builder builder(int tieredStorageBufferSize, String remoteStorageBasePath) {
        return new TieredStorageConfiguration.Builder()
                .setTieredStorageBufferSize(tieredStorageBufferSize)
                .setRemoteStorageBasePath(remoteStorageBasePath);
    }

    public static Builder builder(
            int tieredStorageBufferSize,
            int diskTierMaxRequestBuffers,
            String remoteStorageBasePath) {
        return new TieredStorageConfiguration.Builder()
                .setTieredStorageBufferSize(tieredStorageBufferSize)
                .setDiskTierMaxRequestBuffers(diskTierMaxRequestBuffers)
                .setRemoteStorageBasePath(remoteStorageBasePath);
    }

    public String getRemoteStorageBasePath() {
        return remoteStorageBasePath;
    }

    public int getTieredStorageBufferSize() {
        return tieredStorageBufferSize;
    }

    public int getMemoryTierExclusiveBuffers() {
        return memoryTierExclusiveBuffers;
    }

    public int getDiskTierExclusiveBuffers() {
        return diskTierExclusiveBuffers;
    }

    public int getRemoteTierExclusiveBuffers() {
        return remoteTierExclusiveBuffers;
    }

    public int getNumBuffersUseSortAccumulatorThreshold() {
        return numBuffersUseSortAccumulatorThreshold;
    }

    public int getMemoryTierNumBytesPerSegment() {
        return memoryTierNumBytesPerSegment;
    }

    public int getDiskTierNumBytesPerSegment() {
        return diskTierNumBytesPerSegment;
    }

    public int getRemoteTierNumBytesPerSegment() {
        return remoteTierNumBytesPerSegment;
    }

    public float getNumBuffersTriggerFlushRatio() {
        return numBuffersTriggerFlushRatio;
    }

    public int getDiskTierMaxBuffersReadAhead() {
        return diskTierMaxBuffersReadAhead;
    }

    public Duration getDiskTierBufferRequestTimeout() {
        return diskTierBufferRequestTimeout;
    }

    public int getDiskTierMaxRequestBuffers() {
        return diskTierMaxRequestBuffers;
    }

    public float getMinReserveSpaceFraction() {
        return minReserveSpaceFraction;
    }

    public List<Integer> getTieredStorageTierExclusiveBuffers() {
        List<Integer> exclusiveBuffers = new ArrayList<>();
        exclusiveBuffers.add(memoryTierExclusiveBuffers);
        exclusiveBuffers.add(diskTierExclusiveBuffers);
        exclusiveBuffers.add(remoteTierExclusiveBuffers);
        return exclusiveBuffers;
    }

    public List<TierFactory> getTierFactories() {
        return tierFactories;
    }

    public static class Builder {

        private String remoteStorageBasePath = DEFAULT_REMOTE_STORAGE_BASE_PATH;

        private int tieredStorageBufferSize = DEFAULT_TIERED_STORAGE_BUFFER_SIZE;

        private int memoryTierExclusiveBuffers = DEFAULT_MEMORY_TIER_EXCLUSIVE_BUFFERS;

        private int diskTierExclusiveBuffers = DEFAULT_DISK_TIER_EXCLUSIVE_BUFFERS;

        private int remoteTierExclusiveBuffers = DEFAULT_REMOTE_TIER_EXCLUSIVE_BUFFERS;

        private int numBuffersUseSortAccumulatorThreshold =
                DEFAULT_NUM_BUFFERS_USE_SORT_ACCUMULATOR_THRESHOLD;

        private int memoryTierNumBytesPerSegment = DEFAULT_MEMORY_TIER_NUM_BYTES_PER_SEGMENT;

        private int diskTierNumBytesPerSegment = DEFAULT_DISK_TIER_NUM_BYTES_PER_SEGMENT;

        private int remoteTierNumBytesPerSegment = DEFAULT_REMOTE_TIER_NUM_BYTES_PER_SEGMENT;

        private float numBuffersTriggerFlushRatio = DEFAULT_NUM_BUFFERS_TRIGGER_FLUSH_RATIO;

        private int diskTierMaxBuffersReadAhead = DEFAULT_DISK_TIER_MAX_BUFFERS_READ_AHEAD;

        private Duration diskTierBufferRequestTimeout = DEFAULT_DISK_TIER_BUFFER_REQUEST_TIMEOUT;

        private int diskTierMaxRequestBuffers = DEFAULT_DISK_TIER_MAX_REQUEST_BUFFERS;

        private float minReserveSpaceFraction = DEFAULT_MIN_RESERVE_SPACE_FRACTION;

        private boolean isHybridSelective = DEFAULT_IS_HYBRID_SELECTIVE;

        public Builder setRemoteStorageBasePath(String remoteStorageBasePath) {
            this.remoteStorageBasePath = remoteStorageBasePath;
            return this;
        }

        public Builder setTieredStorageBufferSize(int tieredStorageBufferSize) {
            this.tieredStorageBufferSize = tieredStorageBufferSize;
            return this;
        }

        public Builder setMemoryTierExclusiveBuffers(int memoryTierExclusiveBuffers) {
            this.memoryTierExclusiveBuffers = memoryTierExclusiveBuffers;
            return this;
        }

        public Builder setDiskTierExclusiveBuffers(int diskTierExclusiveBuffers) {
            this.diskTierExclusiveBuffers = diskTierExclusiveBuffers;
            return this;
        }

        public Builder setRemoteTierExclusiveBuffers(int remoteTierExclusiveBuffers) {
            this.remoteTierExclusiveBuffers = remoteTierExclusiveBuffers;
            return this;
        }

        public Builder setNumBuffersUseSortAccumulatorThreshold(
                int numBuffersUseSortAccumulatorThreshold) {
            this.numBuffersUseSortAccumulatorThreshold = numBuffersUseSortAccumulatorThreshold;
            return this;
        }

        public Builder setMemoryTierNumBytesPerSegment(int memoryTierNumBytesPerSegment) {
            this.memoryTierNumBytesPerSegment = memoryTierNumBytesPerSegment;
            return this;
        }

        public Builder setDiskTierNumBytesPerSegment(int diskTierNumBytesPerSegment) {
            this.diskTierNumBytesPerSegment = diskTierNumBytesPerSegment;
            return this;
        }

        public Builder setRemoteTierNumBytesPerSegment(int remoteTierNumBytesPerSegment) {
            this.remoteTierNumBytesPerSegment = remoteTierNumBytesPerSegment;
            return this;
        }

        public Builder setNumBuffersTriggerFlushRatio(float numBuffersTriggerFlushRatio) {
            this.numBuffersTriggerFlushRatio = numBuffersTriggerFlushRatio;
            return this;
        }

        public Builder setDiskTierMaxBuffersReadAhead(int diskTierMaxBuffersReadAhead) {
            this.diskTierMaxBuffersReadAhead = diskTierMaxBuffersReadAhead;
            return this;
        }

        public Builder setDiskTierBufferRequestTimeout(Duration diskTierBufferRequestTimeout) {
            this.diskTierBufferRequestTimeout = diskTierBufferRequestTimeout;
            return this;
        }

        public Builder setDiskTierMaxRequestBuffers(int diskTierMaxRequestBuffers) {
            this.diskTierMaxRequestBuffers = diskTierMaxRequestBuffers;
            return this;
        }

        public Builder setMinReserveSpaceFraction(float minReserveSpaceFraction) {
            this.minReserveSpaceFraction = minReserveSpaceFraction;
            return this;
        }

        public Builder setHybridSelective(boolean hybridSelective) {
            this.isHybridSelective = hybridSelective;
            return this;
        }

        public TieredStorageConfiguration build() {
            return new TieredStorageConfiguration(
                    remoteStorageBasePath,
                    tieredStorageBufferSize,
                    memoryTierExclusiveBuffers,
                    diskTierExclusiveBuffers,
                    remoteTierExclusiveBuffers,
                    numBuffersUseSortAccumulatorThreshold,
                    memoryTierNumBytesPerSegment,
                    diskTierNumBytesPerSegment,
                    remoteTierNumBytesPerSegment,
                    numBuffersTriggerFlushRatio,
                    diskTierMaxBuffersReadAhead,
                    diskTierBufferRequestTimeout,
                    diskTierMaxRequestBuffers,
                    minReserveSpaceFraction,
                    createDefaultFactories());
        }

        private List<TierFactory> createDefaultFactories() {
            List<TierFactory> tierFactories = new ArrayList<>();
            if (isHybridSelective) {
                tierFactories.add(
                        new MemoryTierFactory(
                                memoryTierNumBytesPerSegment, tieredStorageBufferSize));
            }
            tierFactories.add(
                    new DiskTierFactory(
                            diskTierNumBytesPerSegment,
                            tieredStorageBufferSize,
                            minReserveSpaceFraction));
            if (remoteStorageBasePath != null) {
                tierFactories.add(
                        new RemoteTierFactory(
                                remoteTierNumBytesPerSegment,
                                tieredStorageBufferSize,
                                remoteStorageBasePath));
            }
            return tierFactories;
        }
    }
}
