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

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.disk.DiskTierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.memory.MemoryTierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.remote.RemoteTierFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.HYBRID_SELECTIVE;

/** The configuration for TieredStore. */
public class TieredStorageConfiguration {

    /** TODO, only for tests, this should be removed. */
    protected enum TierType {
        IN_MEM,
        IN_DISK,
        IN_REMOTE,
    }

    public static final Map<TierType, Integer> HYBRID_SHUFFLE_TIER_EXCLUSIVE_BUFFERS =
            new HashMap<>();

    static {
        HYBRID_SHUFFLE_TIER_EXCLUSIVE_BUFFERS.put(TierType.IN_MEM, 100);
        HYBRID_SHUFFLE_TIER_EXCLUSIVE_BUFFERS.put(TierType.IN_DISK, 1);
        HYBRID_SHUFFLE_TIER_EXCLUSIVE_BUFFERS.put(TierType.IN_REMOTE, 1);
    }

    private static final int DEFAULT_MEMORY_TIER_NUM_BYTES_PER_SEGMENT = 10 * 32 * 1024; // 320 K

    private static final int DEFAULT_DISK_TIER_NUM_BYTES_PER_SEGMENT = 8 * 1024 * 1024; // 8 M

    private static final int DEFAULT_REMOTE_TIER_NUM_BYTES_PER_SEGMENT =
            32 * 1024 * 1; // 32 k is only for test, expect 8M

    private static final float DEFAULT_MIN_RESERVE_SPACE_FRACTION = 0.05f;

    private static final int DEFAULT_MAX_BUFFERS_READ_AHEAD = 5;

    private static final int DEFAULT_NUM_BUFFERS_USE_SORT_ACCUMULATOR_THRESHOLD =
            99; // TODO chang to 512

    private static final Duration DEFAULT_BUFFER_REQUEST_TIMEOUT = Duration.ofMinutes(5);

    private static final float DEFAULT_TIERED_STORE_BUFFER_IN_MEMORY_RATIO = 0.4f;

    private static final float DEFAULT_TIERED_STORE_FLUSH_BUFFER_RATIO = 0.2f;

    private static final float DEFAULT_TIERED_STORE_TRIGGER_FLUSH_RATIO = 0.8f;

    private static final float DEFAULT_NUM_BUFFERS_TRIGGER_FLUSH_RATIO = 0.6f;

    private static final long DEFAULT_BUFFER_POLL_SIZE_CHECK_INTERVAL_MS = 1000;

    private static final int[] DEFAULT_MEMORY_DISK_EXCLUSIVE_BUFFERS = new int[] {100, 1, 1};

    private static final int[] DEFAULT_MEMORY_DISK_REMOTE_EXCLUSIVE_BUFFERS =
            new int[] {100, 1, 1, 1};

    private final int maxBuffersReadAhead;

    private final Duration bufferRequestTimeout;

    private final int maxRequestedBuffers;

    private final int bufferSize;

    // For Memory Tier
    private final int configuredNetworkBuffersPerChannel;

    private final float tieredStoreBufferInMemoryRatio;

    private final float tieredStoreFlushBufferRatio;

    private final float tieredStoreTriggerFlushRatio;

    private final float numBuffersTriggerFlushRatio;

    private final long bufferPoolSizeCheckIntervalMs;

    private final int numBuffersUseSortAccumulatorThreshold;

    private final String baseDfsHomePath;

    private final int[] tierExclusiveBuffers;

    private final TierFactory[] tierFactories;

    private TieredStorageConfiguration(
            int maxBuffersReadAhead,
            Duration bufferRequestTimeout,
            int maxRequestedBuffers,
            int bufferSize,
            float tieredStoreBufferInMemoryRatio,
            float tieredStoreFlushBufferRatio,
            float tieredStoreTriggerFlushRatio,
            float numBuffersTriggerFlushRatio,
            long bufferPoolSizeCheckIntervalMs,
            int numBuffersUseSortAccumulatorThreshold,
            String baseDfsHomePath,
            int configuredNetworkBuffersPerChannel,
            int[] tierExclusiveBuffers,
            TierFactory[] tierFactories) {
        this.maxBuffersReadAhead = maxBuffersReadAhead;
        this.bufferRequestTimeout = bufferRequestTimeout;
        this.maxRequestedBuffers = maxRequestedBuffers;
        this.bufferSize = bufferSize;
        this.tieredStoreBufferInMemoryRatio = tieredStoreBufferInMemoryRatio;
        this.tieredStoreFlushBufferRatio = tieredStoreFlushBufferRatio;
        this.tieredStoreTriggerFlushRatio = tieredStoreTriggerFlushRatio;
        this.numBuffersTriggerFlushRatio = numBuffersTriggerFlushRatio;
        this.bufferPoolSizeCheckIntervalMs = bufferPoolSizeCheckIntervalMs;
        this.numBuffersUseSortAccumulatorThreshold = numBuffersUseSortAccumulatorThreshold;
        this.baseDfsHomePath = baseDfsHomePath;
        this.configuredNetworkBuffersPerChannel = configuredNetworkBuffersPerChannel;
        this.tierExclusiveBuffers = tierExclusiveBuffers;
        this.tierFactories = tierFactories;
    }

    public static TieredStorageConfiguration.Builder builder() {
        return new TieredStorageConfiguration.Builder();
    }

    public static TieredStorageConfiguration.Builder builder(
            int numSubpartitions, int bufferSize, int numBuffersPerRequest) {
        return new TieredStorageConfiguration.Builder(
                numSubpartitions, bufferSize, numBuffersPerRequest);
    }

    public int getMaxRequestedBuffers() {
        return maxRequestedBuffers;
    }

    /**
     * Determine how many buffers to read ahead at most for each subpartition to prevent other
     * consumers from starving.
     */
    public int getMaxBuffersReadAhead() {
        return maxBuffersReadAhead;
    }

    /**
     * Maximum time to wait when requesting read buffers from the buffer pool before throwing an
     * exception.
     */
    public Duration getBufferRequestTimeout() {
        return bufferRequestTimeout;
    }

    public float getTieredStoreBufferInMemoryRatio() {
        return tieredStoreBufferInMemoryRatio;
    }

    public float getTieredStoreFlushBufferRatio() {
        return tieredStoreFlushBufferRatio;
    }

    public float getTieredStoreTriggerFlushRatio() {
        return tieredStoreTriggerFlushRatio;
    }

    public float getNumBuffersTriggerFlushRatio() {
        return numBuffersTriggerFlushRatio;
    }

    /** Check interval of buffer pool's size. */
    public long getBufferPoolSizeCheckIntervalMs() {
        return bufferPoolSizeCheckIntervalMs;
    }

    public int numBuffersUseSortAccumulatorThreshold() {
        return numBuffersUseSortAccumulatorThreshold;
    }

    public String getBaseDfsHomePath() {
        return baseDfsHomePath;
    }

    public int getConfiguredNetworkBuffersPerChannel() {
        return configuredNetworkBuffersPerChannel;
    }

    public List<TierFactory> getTierFactories() {
        return Arrays.asList(tierFactories);
    }

    public int[] getTierExclusiveBuffers() {
        return tierExclusiveBuffers;
    }

    /** Builder for {@link TieredStorageConfiguration}. */
    public static class Builder {
        private int maxBuffersReadAhead = DEFAULT_MAX_BUFFERS_READ_AHEAD;

        private Duration bufferRequestTimeout = DEFAULT_BUFFER_REQUEST_TIMEOUT;

        private float tieredStoreBufferInMemoryRatio = DEFAULT_TIERED_STORE_BUFFER_IN_MEMORY_RATIO;

        private float tieredStoreFlushBufferRatio = DEFAULT_TIERED_STORE_FLUSH_BUFFER_RATIO;

        private float tieredStoreTriggerFlushRatio = DEFAULT_TIERED_STORE_TRIGGER_FLUSH_RATIO;

        private float numBuffersTriggerFlushRatio = DEFAULT_NUM_BUFFERS_TRIGGER_FLUSH_RATIO;

        private long bufferPoolSizeCheckIntervalMs = DEFAULT_BUFFER_POLL_SIZE_CHECK_INTERVAL_MS;

        private int numBuffersUseSortAccumulatorThreshold =
                DEFAULT_NUM_BUFFERS_USE_SORT_ACCUMULATOR_THRESHOLD;

        private String baseDfsHomePath = null;

        private int configuredNetworkBuffersPerChannel;

        private TierFactory[] tierFactories;

        private int[] tierExclusiveBuffers;

        private int numSubpartitions;

        private int bufferSize;

        private int numBuffersPerRequest;

        private Builder() {}

        private Builder(int numSubpartitions, int bufferSize, int numBuffersPerRequest) {
            this.numSubpartitions = numSubpartitions;
            this.bufferSize = bufferSize;
            this.numBuffersPerRequest = numBuffersPerRequest;
        }

        public TieredStorageConfiguration.Builder setMaxBuffersReadAhead(int maxBuffersReadAhead) {
            this.maxBuffersReadAhead = maxBuffersReadAhead;
            return this;
        }

        public TieredStorageConfiguration.Builder setBufferRequestTimeout(
                Duration bufferRequestTimeout) {
            this.bufferRequestTimeout = bufferRequestTimeout;
            return this;
        }

        public TieredStorageConfiguration.Builder setTieredStoreBufferInMemoryRatio(
                float tieredStoreBufferInMemoryRatio) {
            this.tieredStoreBufferInMemoryRatio = tieredStoreBufferInMemoryRatio;
            return this;
        }

        public TieredStorageConfiguration.Builder setTieredStoreFlushBufferRatio(
                float tieredStoreFlushBufferRatio) {
            this.tieredStoreFlushBufferRatio = tieredStoreFlushBufferRatio;
            return this;
        }

        public TieredStorageConfiguration.Builder setTieredStoreTriggerFlushRatio(
                float tieredStoreTriggerFlushRatio) {
            this.tieredStoreTriggerFlushRatio = tieredStoreTriggerFlushRatio;
            return this;
        }

        public TieredStorageConfiguration.Builder setNumBuffersTriggerFlushRatio(
                float numBuffersTriggerFlushRatio) {
            this.numBuffersTriggerFlushRatio = numBuffersTriggerFlushRatio;
            return this;
        }

        public TieredStorageConfiguration.Builder setBufferPoolSizeCheckIntervalMs(
                long bufferPoolSizeCheckIntervalMs) {
            this.bufferPoolSizeCheckIntervalMs = bufferPoolSizeCheckIntervalMs;
            return this;
        }

        public TieredStorageConfiguration.Builder setNumBuffersUseSortAccumulatorThreshold(
                int numBuffersUseSortAccumulatorThreshold) {
            this.numBuffersUseSortAccumulatorThreshold = numBuffersUseSortAccumulatorThreshold;
            return this;
        }

        public TieredStorageConfiguration.Builder setBaseDfsHomePath(String baseDfsHomePath) {
            this.baseDfsHomePath = baseDfsHomePath;
            return this;
        }

        public TieredStorageConfiguration.Builder setConfiguredNetworkBuffersPerChannel(
                int configuredNetworkBuffersPerChannel) {
            this.configuredNetworkBuffersPerChannel = configuredNetworkBuffersPerChannel;
            return this;
        }

        /** Only for test. This method will be removed in the production code. */
        public TieredStorageConfiguration.Builder setTierTypes(
                String configuredStoreTiers, ResultPartitionType partitionType) {
            TierType[] tierTypes = getConfiguredTierTypes(configuredStoreTiers, partitionType);
            tierFactories = getTierFactories(tierTypes, bufferSize);
            tierExclusiveBuffers = new int[tierTypes.length];

            for (int i = 0; i < tierTypes.length; i++) {
                tierExclusiveBuffers[i] = tierTypes[i] == TierType.IN_MEM ? 100 : 1;
            }
            return this;
        }

        /** Only for test. This method will be removed in the production code. */
        public TieredStorageConfiguration.Builder setTierTypes(String configuredStoreTiers) {
            TierType[] tierTypes = getConfiguredTierTypes(configuredStoreTiers);
            tierFactories = getTierFactories(tierTypes, bufferSize);
            return this;
        }

        /** Only for test. */
        private TierType[] getConfiguredTierTypes(String configuredStoreTiers) {
            switch (configuredStoreTiers) {
                case "MEMORY":
                    return createTierTypes(TierType.IN_MEM);
                case "DISK":
                    return createTierTypes(TierType.IN_DISK);
                case "REMOTE":
                    return createTierTypes(TierType.IN_REMOTE);
                case "MEMORY_DISK":
                    return createTierTypes(TierType.IN_MEM, TierType.IN_DISK);
                case "MEMORY_REMOTE":
                    return createTierTypes(TierType.IN_MEM, TierType.IN_REMOTE);
                case "MEMORY_DISK_REMOTE":
                    return createTierTypes(TierType.IN_MEM, TierType.IN_DISK, TierType.IN_REMOTE);
                case "DISK_REMOTE":
                    return createTierTypes(TierType.IN_DISK, TierType.IN_REMOTE);
                default:
                    throw new IllegalArgumentException(
                            "Illegal tiers combinations for tiered store.");
            }
        }

        private TierType[] getConfiguredTierTypes(
                String configuredStoreTiers, ResultPartitionType partitionType) {
            switch (configuredStoreTiers) {
                case "MEMORY":
                    return createTierTypes(TierType.IN_MEM);
                case "DISK":
                    return createTierTypes(TierType.IN_DISK);
                case "REMOTE":
                    return createTierTypes(TierType.IN_REMOTE);
                case "MEMORY_DISK":
                    return partitionType == HYBRID_SELECTIVE
                            ? createTierTypes(TierType.IN_MEM, TierType.IN_DISK)
                            : createTierTypes(TierType.IN_DISK);
                case "MEMORY_REMOTE":
                    return partitionType == HYBRID_SELECTIVE
                            ? createTierTypes(TierType.IN_MEM, TierType.IN_REMOTE)
                            : createTierTypes(TierType.IN_REMOTE);
                case "MEMORY_DISK_REMOTE":
                    return partitionType == HYBRID_SELECTIVE
                            ? createTierTypes(TierType.IN_MEM, TierType.IN_DISK, TierType.IN_REMOTE)
                            : createTierTypes(TierType.IN_DISK, TierType.IN_REMOTE);

                case "DISK_REMOTE":
                    return createTierTypes(TierType.IN_DISK, TierType.IN_REMOTE);
                default:
                    throw new IllegalArgumentException(
                            "Illegal tiers combinations for tiered store.");
            }
        }

        private TierType[] createTierTypes(TierType... tierTypes) {
            return tierTypes;
        }

        public TieredStorageConfiguration build() {
            if (tierFactories == null) {
                this.tierFactories = getDefaultTierFactories(baseDfsHomePath, bufferSize);
            }
            if (tierExclusiveBuffers == null) {
                this.tierExclusiveBuffers = getDefaultExclusiveBuffers(baseDfsHomePath);
            }

            return new TieredStorageConfiguration(
                    maxBuffersReadAhead,
                    bufferRequestTimeout,
                    Math.max(2 * numBuffersPerRequest, numSubpartitions),
                    bufferSize,
                    tieredStoreBufferInMemoryRatio,
                    tieredStoreFlushBufferRatio,
                    tieredStoreTriggerFlushRatio,
                    numBuffersTriggerFlushRatio,
                    bufferPoolSizeCheckIntervalMs,
                    numBuffersUseSortAccumulatorThreshold,
                    baseDfsHomePath,
                    configuredNetworkBuffersPerChannel,
                    tierExclusiveBuffers,
                    tierFactories);
        }
    }

    // Only for tests
    private static TierFactory[] getTierFactories(TierType[] tierTypes, int bufferSize) {
        TierFactory[] tierFactories = new TierFactory[tierTypes.length];
        for (int i = 0; i < tierTypes.length; i++) {
            tierFactories[i] = createTierFactory(tierTypes[i], bufferSize);
        }
        return tierFactories;
    }

    private static TierFactory createTierFactory(TierType tierType, int bufferSize) {
        switch (tierType) {
            case IN_MEM:
                return new MemoryTierFactory(DEFAULT_MEMORY_TIER_NUM_BYTES_PER_SEGMENT, bufferSize);
            case IN_DISK:
                return new DiskTierFactory(
                        DEFAULT_DISK_TIER_NUM_BYTES_PER_SEGMENT,
                        bufferSize,
                        DEFAULT_MIN_RESERVE_SPACE_FRACTION);
            case IN_REMOTE:
                return new RemoteTierFactory(DEFAULT_REMOTE_TIER_NUM_BYTES_PER_SEGMENT, bufferSize);
            default:
                throw new IllegalArgumentException("Illegal tier type " + tierType);
        }
    }

    private static TierFactory[] getDefaultTierFactories(String baseDfsHomePath, int bufferSize) {
        return baseDfsHomePath == null
                ? new TierFactory[] {
                    new MemoryTierFactory(DEFAULT_MEMORY_TIER_NUM_BYTES_PER_SEGMENT, bufferSize),
                    new DiskTierFactory(
                            DEFAULT_DISK_TIER_NUM_BYTES_PER_SEGMENT,
                            bufferSize,
                            DEFAULT_MIN_RESERVE_SPACE_FRACTION)
                }
                : new TierFactory[] {
                    new MemoryTierFactory(DEFAULT_MEMORY_TIER_NUM_BYTES_PER_SEGMENT, bufferSize),
                    new DiskTierFactory(
                            DEFAULT_DISK_TIER_NUM_BYTES_PER_SEGMENT,
                            bufferSize,
                            DEFAULT_MIN_RESERVE_SPACE_FRACTION),
                    new RemoteTierFactory(DEFAULT_REMOTE_TIER_NUM_BYTES_PER_SEGMENT, bufferSize)
                };
    }

    private static int[] getDefaultExclusiveBuffers(String baseDfsHomePath) {
        return baseDfsHomePath == null
                ? DEFAULT_MEMORY_DISK_EXCLUSIVE_BUFFERS
                : DEFAULT_MEMORY_DISK_REMOTE_EXCLUSIVE_BUFFERS;
    }
}
