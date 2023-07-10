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

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.BufferAccumulator;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.HashBufferAccumulator;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.SortBufferAccumulator;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierProducerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.disk.DiskIOScheduler;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.disk.DiskTierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.memory.MemoryTierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.remote.RemoteTierFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/** Configurations for tiered storage. */
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

    private final String remoteStorageBasePath;

    private final int tieredStorageBufferSize;

    private final int memoryTierExclusiveBuffers;

    private final int diskTierExclusiveBuffers;

    private final int remoteTierExclusiveBuffers;

    private final int accumulatorExclusiveBuffers;

    private final int memoryTierNumBytesPerSegment;

    private final int diskTierNumBytesPerSegment;

    private final int remoteTierNumBytesPerSegment;

    private final float numBuffersTriggerFlushRatio;

    private final int diskIOSchedulerMaxBuffersReadAhead;

    private final Duration diskIOSchedulerRequestTimeout;

    private final int diskIOSchedulerMaxRequestBuffers;

    private final float minReserveSpaceFraction;

    private final List<TierFactory> tierFactories;

    public TieredStorageConfiguration(
            String remoteStorageBasePath,
            int tieredStorageBufferSize,
            int memoryTierExclusiveBuffers,
            int diskTierExclusiveBuffers,
            int remoteTierExclusiveBuffers,
            int accumulatorExclusiveBuffers,
            int memoryTierNumBytesPerSegment,
            int diskTierNumBytesPerSegment,
            int remoteTierNumBytesPerSegment,
            float numBuffersTriggerFlushRatio,
            int diskIOSchedulerMaxBuffersReadAhead,
            Duration diskIOSchedulerRequestTimeout,
            int diskIOSchedulerMaxRequestBuffers,
            float minReserveSpaceFraction,
            List<TierFactory> tierFactories) {
        this.remoteStorageBasePath = remoteStorageBasePath;
        this.tieredStorageBufferSize = tieredStorageBufferSize;
        this.memoryTierExclusiveBuffers = memoryTierExclusiveBuffers;
        this.diskTierExclusiveBuffers = diskTierExclusiveBuffers;
        this.remoteTierExclusiveBuffers = remoteTierExclusiveBuffers;
        this.accumulatorExclusiveBuffers = accumulatorExclusiveBuffers;
        this.memoryTierNumBytesPerSegment = memoryTierNumBytesPerSegment;
        this.diskTierNumBytesPerSegment = diskTierNumBytesPerSegment;
        this.remoteTierNumBytesPerSegment = remoteTierNumBytesPerSegment;
        this.numBuffersTriggerFlushRatio = numBuffersTriggerFlushRatio;
        this.diskIOSchedulerMaxBuffersReadAhead = diskIOSchedulerMaxBuffersReadAhead;
        this.diskIOSchedulerRequestTimeout = diskIOSchedulerRequestTimeout;
        this.diskIOSchedulerMaxRequestBuffers = diskIOSchedulerMaxRequestBuffers;
        this.minReserveSpaceFraction = minReserveSpaceFraction;
        this.tierFactories = tierFactories;
    }

    public static Builder builder(String remoteStorageBasePath) {
        return new TieredStorageConfiguration.Builder()
                .setRemoteStorageBasePath(remoteStorageBasePath);
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

    /**
     * Get the base path on remote storage.
     *
     * @return string if the remote storage path is configured otherwise null.
     */
    public String getRemoteStorageBasePath() {
        return remoteStorageBasePath;
    }

    /**
     * Get the buffer size in tiered storage.
     *
     * @return the buffer size.
     */
    public int getTieredStorageBufferSize() {
        return tieredStorageBufferSize;
    }

    /**
     * Get exclusive buffer number of memory tier.
     *
     * @return the buffer number.
     */
    public int getMemoryTierExclusiveBuffers() {
        return memoryTierExclusiveBuffers;
    }

    /**
     * Get exclusive buffer number of disk tier.
     *
     * @return the buffer number.
     */
    public int getDiskTierExclusiveBuffers() {
        return diskTierExclusiveBuffers;
    }

    /**
     * Get exclusive buffer number of remote tier.
     *
     * @return the buffer number.
     */
    public int getRemoteTierExclusiveBuffers() {
        return remoteTierExclusiveBuffers;
    }

    /**
     * Get exclusive buffer number of accumulator.
     *
     * <p>The buffer number is used to compare with the subpartition number to determine the type of
     * {@link BufferAccumulator}.
     *
     * <p>If the exclusive buffer number is larger than (subpartitionNum + 1), the accumulator will
     * use {@link HashBufferAccumulator}. If the exclusive buffer number is equal to or smaller than
     * (subpartitionNum + 1), the accumulator will use {@link SortBufferAccumulator}
     *
     * @return the buffer number.
     */
    public int getAccumulatorExclusiveBuffers() {
        return accumulatorExclusiveBuffers;
    }

    /**
     * Get the segment size of memory tier.
     *
     * @return segment size.
     */
    public int getMemoryTierNumBytesPerSegment() {
        return memoryTierNumBytesPerSegment;
    }

    /**
     * Get the segment size of disk tier.
     *
     * @return segment size.
     */
    public int getDiskTierNumBytesPerSegment() {
        return diskTierNumBytesPerSegment;
    }

    /**
     * Get the segment size of remote tier.
     *
     * @return segment size.
     */
    public int getRemoteTierNumBytesPerSegment() {
        return remoteTierNumBytesPerSegment;
    }

    /**
     * When the number of buffers that have been requested exceeds this threshold, trigger the
     * flushing operation in each {@link TierProducerAgent}.
     *
     * @return flush ratio.
     */
    public float getNumBuffersTriggerFlushRatio() {
        return numBuffersTriggerFlushRatio;
    }

    /**
     * The number of buffers to read ahead at most for each subpartition in {@link DiskIOScheduler},
     * which can be used to prevent other consumers from starving.
     *
     * @return buffer number.
     */
    public int getDiskIOSchedulerMaxBuffersReadAhead() {
        return diskIOSchedulerMaxBuffersReadAhead;
    }

    /**
     * Maximum time to wait when requesting read buffers from the buffer pool before throwing an
     * exception in {@link DiskIOScheduler}.
     *
     * @return timeout duration.
     */
    public Duration getDiskIOSchedulerBufferRequestTimeout() {
        return diskIOSchedulerRequestTimeout;
    }

    /**
     * Maximum buffer number that can be allocated to a single {@link DiskIOScheduler}.
     *
     * @return buffer number.
     */
    public int getDiskTierIOSchedulerMaxRequestBuffers() {
        return diskIOSchedulerMaxRequestBuffers;
    }

    /**
     * Minimum reserved disk space fraction in disk tier.
     *
     * @return the fraction.
     */
    public float getMinReserveSpaceFraction() {
        return minReserveSpaceFraction;
    }

    /**
     * Get the total exclusive buffer number.
     *
     * @return the total exclusive buffer number.
     */
    public int getTotalExclusiveBuffers() {
        return memoryTierExclusiveBuffers
                + diskTierExclusiveBuffers
                + remoteTierExclusiveBuffers
                + accumulatorExclusiveBuffers;
    }

    public List<TierFactory> getTierFactories() {
        return tierFactories;
    }

    /** The builder for {@link TieredStorageConfiguration}. */
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
            tierFactories.add(
                    new MemoryTierFactory(memoryTierNumBytesPerSegment, tieredStorageBufferSize));
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
