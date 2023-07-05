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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
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

    private static final float DEFAULT_NUM_BUFFERS_TRIGGER_FLUSH_RATIO = 0.6f;

    private static final int[] DEFAULT_MEMORY_DISK_EXCLUSIVE_BUFFERS = new int[] {100, 1, 1};

    private static final int[] DEFAULT_MEMORY_DISK_REMOTE_EXCLUSIVE_BUFFERS =
            new int[] {100, 1, 1, 1};

    private final int maxBuffersReadAhead;

    private final Duration bufferRequestTimeout;

    private final int maxRequestedBuffers;

    private final float numBuffersTriggerFlushRatio;

    private final int numBuffersUseSortAccumulatorThreshold;

    private final int[] tierExclusiveBuffers;

    private final TierFactory[] tierFactories;

    private TieredStorageConfiguration(
            int maxBuffersReadAhead,
            Duration bufferRequestTimeout,
            int maxRequestedBuffers,
            float numBuffersTriggerFlushRatio,
            int numBuffersUseSortAccumulatorThreshold,
            int[] tierExclusiveBuffers,
            TierFactory[] tierFactories) {
        this.maxBuffersReadAhead = maxBuffersReadAhead;
        this.bufferRequestTimeout = bufferRequestTimeout;
        this.maxRequestedBuffers = maxRequestedBuffers;
        this.numBuffersTriggerFlushRatio = numBuffersTriggerFlushRatio;
        this.numBuffersUseSortAccumulatorThreshold = numBuffersUseSortAccumulatorThreshold;
        // For Memory Tier
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


    public int getMaxBuffersReadAhead() {
        return maxBuffersReadAhead;
    }


    public Duration getBufferRequestTimeout() {
        return bufferRequestTimeout;
    }

    public float getNumBuffersTriggerFlushRatio() {
        return numBuffersTriggerFlushRatio;
    }

    public int numBuffersUseSortAccumulatorThreshold() {
        return numBuffersUseSortAccumulatorThreshold;
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

        private float numBuffersTriggerFlushRatio = DEFAULT_NUM_BUFFERS_TRIGGER_FLUSH_RATIO;

        private int numBuffersUseSortAccumulatorThreshold =
                DEFAULT_NUM_BUFFERS_USE_SORT_ACCUMULATOR_THRESHOLD;

        private String remoteStorageBasePath = null;

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

        public TieredStorageConfiguration.Builder setRemoteStorageBasePath(
                String remoteStorageBasePath) {
            this.remoteStorageBasePath = remoteStorageBasePath;
            return this;
        }

        /** Only for test. This method will be removed in the production code. */
        public TieredStorageConfiguration.Builder setTierTypes(
                String configuredStoreTiers,
                ResultPartitionType partitionType,
                String remoteStorageBasePath) {
            TierType[] tierTypes = getConfiguredTierTypes(configuredStoreTiers, partitionType);
            tierFactories = getTierFactories(tierTypes, bufferSize, remoteStorageBasePath);
            tierExclusiveBuffers = new int[tierTypes.length];

            for (int i = 0; i < tierTypes.length; i++) {
                tierExclusiveBuffers[i] = tierTypes[i] == TierType.IN_MEM ? 100 : 1;
            }
            return this;
        }

        /** Only for test. This method will be removed in the production code. */
        public TieredStorageConfiguration.Builder setTierTypes(
                String configuredStoreTiers, String remoteStorageBasePath) {
            TierType[] tierTypes = getConfiguredTierTypes(configuredStoreTiers);
            tierFactories = getTierFactories(tierTypes, bufferSize, remoteStorageBasePath);
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
                this.tierFactories = getDefaultTierFactories(remoteStorageBasePath, bufferSize);
            }
            if (tierExclusiveBuffers == null) {
                this.tierExclusiveBuffers = getDefaultExclusiveBuffers(remoteStorageBasePath);
            }

            return new TieredStorageConfiguration(
                    maxBuffersReadAhead,
                    bufferRequestTimeout,
                    Math.max(2 * numBuffersPerRequest, numSubpartitions),
                    numBuffersTriggerFlushRatio,
                    numBuffersUseSortAccumulatorThreshold,
                    tierExclusiveBuffers,
                    tierFactories);
        }
    }

    // Only for tests
    private static TierFactory[] getTierFactories(
            TierType[] tierTypes, int bufferSize, String remoteStorageBasePath) {
        TierFactory[] tierFactories = new TierFactory[tierTypes.length];
        for (int i = 0; i < tierTypes.length; i++) {
            tierFactories[i] = createTierFactory(tierTypes[i], bufferSize, remoteStorageBasePath);
        }
        return tierFactories;
    }

    private static TierFactory createTierFactory(
            TierType tierType, int bufferSize, String remoteStorageBasePath) {
        switch (tierType) {
            case IN_MEM:
                return new MemoryTierFactory(DEFAULT_MEMORY_TIER_NUM_BYTES_PER_SEGMENT, bufferSize);
            case IN_DISK:
                return new DiskTierFactory(
                        DEFAULT_DISK_TIER_NUM_BYTES_PER_SEGMENT,
                        bufferSize,
                        DEFAULT_MIN_RESERVE_SPACE_FRACTION);
            case IN_REMOTE:
                return new RemoteTierFactory(
                        DEFAULT_REMOTE_TIER_NUM_BYTES_PER_SEGMENT,
                        bufferSize,
                        remoteStorageBasePath);
            default:
                throw new IllegalArgumentException("Illegal tier type " + tierType);
        }
    }

    private static TierFactory[] getDefaultTierFactories(
            String remoteStorageBasePath, int bufferSize) {
        return remoteStorageBasePath == null
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
                    new RemoteTierFactory(
                            DEFAULT_REMOTE_TIER_NUM_BYTES_PER_SEGMENT,
                            bufferSize,
                            remoteStorageBasePath)
                };
    }

    private static int[] getDefaultExclusiveBuffers(String baseDfsHomePath) {
        return baseDfsHomePath == null
                ? DEFAULT_MEMORY_DISK_EXCLUSIVE_BUFFERS
                : DEFAULT_MEMORY_DISK_REMOTE_EXCLUSIVE_BUFFERS;
    }

    public static TieredStorageConfiguration fromConfiguration(Configuration conf) {
        String remoteStorageBasePath =
                conf.getString(
                        NettyShuffleEnvironmentOptions
                                .NETWORK_HYBRID_SHUFFLE_REMOTE_STORAGE_BASE_PATH);
        TieredStorageConfiguration.Builder builder = new TieredStorageConfiguration.Builder();
        builder.setRemoteStorageBasePath(remoteStorageBasePath);
        return builder.build();
    }
}
