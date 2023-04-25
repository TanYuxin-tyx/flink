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

import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.hybrid.HsFullSpillingStrategy;
import org.apache.flink.runtime.io.network.partition.hybrid.HsSelectiveSpillingStrategy;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.shuffle.TierType;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TierMemorySpec;
import org.apache.flink.runtime.shuffle.NettyShuffleUtils;
import org.apache.flink.util.StringUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.HYBRID_SELECTIVE;
import static org.apache.flink.util.Preconditions.checkState;

/** The configuration for TieredStore. */
public class TieredStorageConfiguration {

    static TierType[] allTierTypes =
            new TierType[] {TierType.IN_MEM, TierType.IN_DISK, TierType.IN_REMOTE};

    private static final int DEFAULT_MAX_BUFFERS_READ_AHEAD = 5;

    private static final Duration DEFAULT_BUFFER_REQUEST_TIMEOUT = Duration.ofMinutes(5);

    private static final float DEFAULT_SELECTIVE_STRATEGY_SPILL_THRESHOLD = 0.7f;

    private static final float DEFAULT_SELECTIVE_STRATEGY_SPILL_BUFFER_RATIO = 0.4f;

    private static final float DEFAULT_FULL_STRATEGY_NUM_BUFFERS_TRIGGER_SPILLED_RATIO = 0.5f;

    private static final float DEFAULT_FULL_STRATEGY_RELEASE_THRESHOLD = 0.7f;

    private static final float DEFAULT_FULL_STRATEGY_RELEASE_BUFFER_RATIO = 0.4f;

    private static final float DEFAULT_TIERED_STORE_BUFFER_IN_MEMORY_RATIO = 0.4f;

    private static final float DEFAULT_TIERED_STORE_FLUSH_BUFFER_RATIO = 0.2f;

    private static final float DEFAULT_TIERED_STORE_TRIGGER_FLUSH_RATIO = 0.8f;

    private static final float DEFAULT_NUM_BUFFERS_TRIGGER_FLUSH_RATIO = 0.6f;

    private static final long DEFAULT_BUFFER_POLL_SIZE_CHECK_INTERVAL_MS = 1000;

    private static final TierType[] MEMORY_ONLY_TIER_TYPE = new TierType[] {TierType.IN_MEM};

    private static final TierType[] MEMORY_DISK_TIER_TYPES =
            new TierType[] {TierType.IN_MEM, TierType.IN_DISK};

    private static final TierType[] MEMORY_DISK_REMOTE_TIER_TYPES =
            new TierType[] {TierType.IN_MEM, TierType.IN_DISK, TierType.IN_REMOTE};

    private final int maxBuffersReadAhead;

    private final Duration bufferRequestTimeout;

    private final int maxRequestedBuffers;

    // For Memory Tier
    private final int configuredNetworkBuffersPerChannel;

    // ----------------------------------------
    //        Selective Spilling Strategy
    // ----------------------------------------
    private final float selectiveStrategySpillThreshold;

    private final float selectiveStrategySpillBufferRatio;

    // ----------------------------------------
    //        Full Spilling Strategy
    // ----------------------------------------
    private final float fullStrategyNumBuffersTriggerSpillingRatio;

    private final float fullStrategyReleaseThreshold;

    private final float fullStrategyReleaseBufferRatio;

    private final float tieredStoreBufferInMemoryRatio;

    private final float tieredStoreFlushBufferRatio;

    private final float tieredStoreTriggerFlushRatio;

    private final float numBuffersTriggerFlushRatio;

    private final long bufferPoolSizeCheckIntervalMs;

    private final String baseDfsHomePath;

    private final TierType[] tierTypes;

    private final TierType[] upstreamTierTypes;

    private final TierType[] remoteTierTypes;

    private final List<TierConfSpec> tierConfSpecs;
    private final List<TierMemorySpec> tierMemorySpecs;

    private TieredStorageConfiguration(
            int maxBuffersReadAhead,
            Duration bufferRequestTimeout,
            int maxRequestedBuffers,
            float selectiveStrategySpillThreshold,
            float selectiveStrategySpillBufferRatio,
            float fullStrategyNumBuffersTriggerSpillingRatio,
            float fullStrategyReleaseThreshold,
            float fullStrategyReleaseBufferRatio,
            float tieredStoreBufferInMemoryRatio,
            float tieredStoreFlushBufferRatio,
            float tieredStoreTriggerFlushRatio,
            float numBuffersTriggerFlushRatio,
            long bufferPoolSizeCheckIntervalMs,
            String baseDfsHomePath,
            int configuredNetworkBuffersPerChannel,
            TierType[] tierTypes,
            TierType[] upstreamTierTypes,
            TierType[] remoteTierTypes,
            List<TierConfSpec> tierConfSpecs,
            List<TierMemorySpec> tierMemorySpecs) {
        this.maxBuffersReadAhead = maxBuffersReadAhead;
        this.bufferRequestTimeout = bufferRequestTimeout;
        this.maxRequestedBuffers = maxRequestedBuffers;
        this.selectiveStrategySpillThreshold = selectiveStrategySpillThreshold;
        this.selectiveStrategySpillBufferRatio = selectiveStrategySpillBufferRatio;
        this.fullStrategyNumBuffersTriggerSpillingRatio =
                fullStrategyNumBuffersTriggerSpillingRatio;
        this.fullStrategyReleaseThreshold = fullStrategyReleaseThreshold;
        this.fullStrategyReleaseBufferRatio = fullStrategyReleaseBufferRatio;
        this.tieredStoreBufferInMemoryRatio = tieredStoreBufferInMemoryRatio;
        this.tieredStoreFlushBufferRatio = tieredStoreFlushBufferRatio;
        this.tieredStoreTriggerFlushRatio = tieredStoreTriggerFlushRatio;
        this.numBuffersTriggerFlushRatio = numBuffersTriggerFlushRatio;
        this.bufferPoolSizeCheckIntervalMs = bufferPoolSizeCheckIntervalMs;
        this.baseDfsHomePath = baseDfsHomePath;
        this.configuredNetworkBuffersPerChannel = configuredNetworkBuffersPerChannel;
        this.tierTypes = tierTypes;
        this.upstreamTierTypes = upstreamTierTypes;
        this.remoteTierTypes = remoteTierTypes;
        this.tierConfSpecs = tierConfSpecs;
        this.tierMemorySpecs = tierMemorySpecs;
    }

    public static TieredStorageConfiguration.Builder builder(
            int numSubpartitions, int numBuffersPerRequest) {
        return new TieredStorageConfiguration.Builder(numSubpartitions, numBuffersPerRequest);
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

    /**
     * When the number of buffers that have been requested exceeds this threshold, trigger the
     * spilling operation. Used by {@link HsSelectiveSpillingStrategy}.
     */
    public float getSelectiveStrategySpillThreshold() {
        return selectiveStrategySpillThreshold;
    }

    /** The proportion of buffers to be spilled. Used by {@link HsSelectiveSpillingStrategy}. */
    public float getSelectiveStrategySpillBufferRatio() {
        return selectiveStrategySpillBufferRatio;
    }

    /**
     * When the number of unSpilled buffers equal to this ratio times pool size, trigger the
     * spilling operation. Used by {@link HsFullSpillingStrategy}.
     */
    public float getFullStrategyNumBuffersTriggerSpillingRatio() {
        return fullStrategyNumBuffersTriggerSpillingRatio;
    }

    /**
     * When the number of buffers that have been requested exceeds this threshold, trigger the
     * release operation. Used by {@link HsFullSpillingStrategy}.
     */
    public float getFullStrategyReleaseThreshold() {
        return fullStrategyReleaseThreshold;
    }

    /** The proportion of buffers to be released. Used by {@link HsFullSpillingStrategy}. */
    public float getFullStrategyReleaseBufferRatio() {
        return fullStrategyReleaseBufferRatio;
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

    public String getBaseDfsHomePath() {
        return baseDfsHomePath;
    }

    public int getConfiguredNetworkBuffersPerChannel() {
        return configuredNetworkBuffersPerChannel;
    }

    public TierType[] getTierTypes() {
        return tierTypes;
    }

    public TierType[] getUpstreamTierTypes() {
        return upstreamTierTypes;
    }

    public int[] getTierIndexes() {
        // TODO, use indexes directly
        int[] tierIndexes = new int[upstreamTierTypes.length];
        for (int i = 0; i < upstreamTierTypes.length; i++) {
            if (upstreamTierTypes[i] == TierType.IN_MEM) {
                tierIndexes[i] = 0;
            } else if (upstreamTierTypes[i] == TierType.IN_DISK) {
                tierIndexes[i] = 1;
            } else if (upstreamTierTypes[i] == TierType.IN_REMOTE) {
                tierIndexes[i] = 2;
            }
        }
        return tierIndexes;
    }

    public TierType[] getRemoteTierTypes() {
        return remoteTierTypes;
    }

    public int[] getRemoteTierIndexes() {
        // TODO, use indexes directly
        int[] tierIndexes = new int[remoteTierTypes.length];
        for (int i = 0; i < remoteTierTypes.length; i++) {
            if (remoteTierTypes[i] == TierType.IN_MEM) {
                tierIndexes[i] = 0;
            } else if (remoteTierTypes[i] == TierType.IN_DISK) {
                tierIndexes[i] = 1;
            } else if (remoteTierTypes[i] == TierType.IN_REMOTE) {
                tierIndexes[i] = 2;
            }
        }
        return tierIndexes;
    }

    public static TierType[] memoryOnlyTierType() {
        return MEMORY_ONLY_TIER_TYPE;
    }

    public static TierType[] memoryDiskTierTypes() {
        return MEMORY_DISK_TIER_TYPES;
    }

    public static TierType[] memoryDiskRemoteTierTypes() {
        return MEMORY_DISK_REMOTE_TIER_TYPES;
    }

    public List<TierConfSpec> getTierConfSpecs() {
        return tierConfSpecs;
    }

    public List<TierMemorySpec> getTierMemorySpecs() {
        return tierMemorySpecs;
    }

    /** Builder for {@link TieredStorageConfiguration}. */
    public static class Builder {
        private int maxBuffersReadAhead = DEFAULT_MAX_BUFFERS_READ_AHEAD;

        private Duration bufferRequestTimeout = DEFAULT_BUFFER_REQUEST_TIMEOUT;

        private float selectiveStrategySpillThreshold = DEFAULT_SELECTIVE_STRATEGY_SPILL_THRESHOLD;

        private float selectiveStrategySpillBufferRatio =
                DEFAULT_SELECTIVE_STRATEGY_SPILL_BUFFER_RATIO;

        private float fullStrategyNumBuffersTriggerSpillingRatio =
                DEFAULT_FULL_STRATEGY_NUM_BUFFERS_TRIGGER_SPILLED_RATIO;

        private float fullStrategyReleaseThreshold = DEFAULT_FULL_STRATEGY_RELEASE_THRESHOLD;

        private float fullStrategyReleaseBufferRatio = DEFAULT_FULL_STRATEGY_RELEASE_BUFFER_RATIO;

        private float tieredStoreBufferInMemoryRatio = DEFAULT_TIERED_STORE_BUFFER_IN_MEMORY_RATIO;

        private float tieredStoreFlushBufferRatio = DEFAULT_TIERED_STORE_FLUSH_BUFFER_RATIO;

        private float tieredStoreTriggerFlushRatio = DEFAULT_TIERED_STORE_TRIGGER_FLUSH_RATIO;

        private float numBuffersTriggerFlushRatio = DEFAULT_NUM_BUFFERS_TRIGGER_FLUSH_RATIO;

        private long bufferPoolSizeCheckIntervalMs = DEFAULT_BUFFER_POLL_SIZE_CHECK_INTERVAL_MS;

        private String baseDfsHomePath = null;

        private int configuredNetworkBuffersPerChannel;

        private TierType[] tierTypes;

        private int[] tierIndexes;

        private TierType[] upstreamTierTypes;

        private TierType[] remoteTierTypes;

        private String tieredStoreSpillingType;

        private final int numSubpartitions;

        private List<TierConfSpec> tierConfSpecs;

        private final List<TierMemorySpec> tierMemorySpecs = new ArrayList<>();

        private final int numBuffersPerRequest;

        private Builder(int numSubpartitions, int numBuffersPerRequest) {
            this.numSubpartitions = numSubpartitions;
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

        public TieredStorageConfiguration.Builder setSelectiveStrategySpillThreshold(
                float selectiveStrategySpillThreshold) {
            this.selectiveStrategySpillThreshold = selectiveStrategySpillThreshold;
            return this;
        }

        public TieredStorageConfiguration.Builder setSelectiveStrategySpillBufferRatio(
                float selectiveStrategySpillBufferRatio) {
            this.selectiveStrategySpillBufferRatio = selectiveStrategySpillBufferRatio;
            return this;
        }

        public TieredStorageConfiguration.Builder setFullStrategyNumBuffersTriggerSpillingRatio(
                float fullStrategyNumBuffersTriggerSpillingRatio) {
            this.fullStrategyNumBuffersTriggerSpillingRatio =
                    fullStrategyNumBuffersTriggerSpillingRatio;
            return this;
        }

        public TieredStorageConfiguration.Builder setFullStrategyReleaseThreshold(
                float fullStrategyReleaseThreshold) {
            this.fullStrategyReleaseThreshold = fullStrategyReleaseThreshold;
            return this;
        }

        public TieredStorageConfiguration.Builder setFullStrategyReleaseBufferRatio(
                float fullStrategyReleaseBufferRatio) {
            this.fullStrategyReleaseBufferRatio = fullStrategyReleaseBufferRatio;
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

        public TieredStorageConfiguration.Builder setBaseDfsHomePath(String baseDfsHomePath) {
            this.baseDfsHomePath = baseDfsHomePath;
            return this;
        }

        public TieredStorageConfiguration.Builder setConfiguredNetworkBuffersPerChannel(
                int configuredNetworkBuffersPerChannel) {
            this.configuredNetworkBuffersPerChannel = configuredNetworkBuffersPerChannel;
            return this;
        }

        public TieredStorageConfiguration.Builder setTierTypes(
                String configuredStoreTiers, ResultPartitionType partitionType) {
            this.tierTypes = getConfiguredTierTypes(configuredStoreTiers, partitionType);
            this.upstreamTierTypes =
                    getConfiguredUpstreamTierTypes(configuredStoreTiers, partitionType);
            this.remoteTierTypes =
                    getConfiguredRemoteTierTypes(configuredStoreTiers, partitionType);
            // After the method is replaced, these generation of tierConfSpecs is useless.
            this.tierConfSpecs = new ArrayList<>();
            for (int i = 0; i < tierTypes.length; i++) {
                tierConfSpecs.add(
                        new TierConfSpec(
                                tierTypes[i],
                                NettyShuffleUtils.HYBRID_SHUFFLE_TIER_EXCLUSIVE_BUFFERS.get(
                                        tierTypes[i]),
                                tierTypes[i] != TierType.IN_MEM));
            }
            return this;
        }

        public TieredStorageConfiguration.Builder setTierTypes(TierType[] tierTypes) {
            this.tierTypes = tierTypes;
            return this;
        }

        public TieredStorageConfiguration.Builder setTierSpecs(List<TierConfSpec> tierConfSpecs) {
            this.tierConfSpecs = tierConfSpecs;
            this.tierTypes = new TierType[tierConfSpecs.size()];
            this.tierIndexes = new int[tierConfSpecs.size()];
            for (int i = 0; i < tierConfSpecs.size(); i++) {
                tierTypes[i] = tierConfSpecs.get(i).getTierType();
                tierIndexes[i] = getTierIndexFromType(tierTypes[i]);
                TierMemorySpec tierMemorySpec =
                        new TierMemorySpec(
                                i,
                                tierConfSpecs.get(i).getNumExclusiveBuffers(),
                                tierConfSpecs.get(i).canUseSharedBuffers());
                tierMemorySpecs.add(tierMemorySpec);
            }
            return this;
        }

        public TieredStorageConfiguration.Builder setDefaultTierMemorySpecs() {
            tierMemorySpecs.clear();
            for (int i = 0; i < tierTypes.length; i++) {
                int numExclusive =
                        NettyShuffleUtils.HYBRID_SHUFFLE_TIER_EXCLUSIVE_BUFFERS.get(tierTypes[i]);
                tierMemorySpecs.add(
                        new TierMemorySpec(i, numExclusive, tierTypes[i] != TierType.IN_MEM));
            }
            return this;
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

        private TierType[] getConfiguredUpstreamTierTypes(
                String configuredStoreTiers, ResultPartitionType partitionType) {
            switch (configuredStoreTiers) {
                case "MEMORY":
                    return createTierTypes(TierType.IN_MEM);
                case "DISK":
                case "DISK_REMOTE":
                    return createTierTypes(TierType.IN_DISK);
                case "REMOTE":
                    return new TierType[0];
                case "MEMORY_DISK":
                case "MEMORY_DISK_REMOTE":
                    return partitionType == HYBRID_SELECTIVE
                            ? createTierTypes(TierType.IN_MEM, TierType.IN_DISK)
                            : createTierTypes(TierType.IN_DISK);
                case "MEMORY_REMOTE":
                    return partitionType == HYBRID_SELECTIVE
                            ? createTierTypes(TierType.IN_MEM)
                            : new TierType[0];
                default:
                    throw new IllegalArgumentException(
                            "Illegal tiers combinations for tiered store.");
            }
        }

        private TierType[] getConfiguredRemoteTierTypes(
                String configuredStoreTiers, ResultPartitionType partitionType) {
            switch (configuredStoreTiers) {
                case "MEMORY":
                case "DISK":
                case "MEMORY_DISK":
                    return new TierType[0];
                case "REMOTE":
                case "MEMORY_REMOTE":
                case "MEMORY_DISK_REMOTE":
                case "DISK_REMOTE":
                    return createTierTypes(TierType.IN_REMOTE);
                default:
                    throw new IllegalArgumentException(
                            "Illegal tiers combinations for tiered store.");
            }
        }

        private TierType[] createTierTypes(TierType... tierTypes) {
            return tierTypes;
        }

        public TieredStorageConfiguration build() {
            validateConfiguredOptions();
            return new TieredStorageConfiguration(
                    maxBuffersReadAhead,
                    bufferRequestTimeout,
                    Math.max(2 * numBuffersPerRequest, numSubpartitions),
                    selectiveStrategySpillThreshold,
                    selectiveStrategySpillBufferRatio,
                    fullStrategyNumBuffersTriggerSpillingRatio,
                    fullStrategyReleaseThreshold,
                    fullStrategyReleaseBufferRatio,
                    tieredStoreBufferInMemoryRatio,
                    tieredStoreFlushBufferRatio,
                    tieredStoreTriggerFlushRatio,
                    numBuffersTriggerFlushRatio,
                    bufferPoolSizeCheckIntervalMs,
                    baseDfsHomePath,
                    configuredNetworkBuffersPerChannel,
                    tierTypes,
                    upstreamTierTypes,
                    remoteTierTypes,
                    tierConfSpecs,
                    tierMemorySpecs);
        }

        private void validateConfiguredOptions() {
            if (remoteTierTypes.length > 0 && StringUtils.isNullOrWhitespaceOnly(baseDfsHomePath)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Must specify remote storage home path by %s when using DFS in Tiered Store.",
                                NettyShuffleEnvironmentOptions
                                        .NETWORK_HYBRID_SHUFFLE_REMOTE_STORAGE_BASE_HOME_PATH
                                        .key()));
            }
            checkState(tierTypes.length == upstreamTierTypes.length + remoteTierTypes.length);
        }
    }

    private static int getTierIndexFromType(TierType tierType) {
        for (int i = 0; i < allTierTypes.length; i++) {
            if (tierType == allTierTypes[i]) {
                return i;
            }
        }
        throw new IllegalArgumentException("No such a tier type " + tierType);
    }
}
