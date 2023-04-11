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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.hybrid.HsFullSpillingStrategy;
import org.apache.flink.runtime.io.network.partition.hybrid.HsSelectiveSpillingStrategy;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.TierType;

import java.time.Duration;

import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.HYBRID_SELECTIVE;
import static org.apache.flink.util.Preconditions.checkState;

/** The configuration for TieredStore. */
public class TieredStoreConfiguration {

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

    private TieredStoreConfiguration(
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
            TierType[] remoteTierTypes) {
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
    }

    public static TieredStoreConfiguration.Builder builder(
            int numSubpartitions, int numBuffersPerRequest) {
        return new TieredStoreConfiguration.Builder(numSubpartitions, numBuffersPerRequest);
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

    public TierType[] getRemoteTierTypes() {
        return remoteTierTypes;
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

    /** Builder for {@link TieredStoreConfiguration}. */
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

        private TierType[] upstreamTierTypes;

        private TierType[] remoteTierTypes;

        private String tieredStoreSpillingType;

        private final int numSubpartitions;

        private final int numBuffersPerRequest;

        private Builder(int numSubpartitions, int numBuffersPerRequest) {
            this.numSubpartitions = numSubpartitions;
            this.numBuffersPerRequest = numBuffersPerRequest;
        }

        public TieredStoreConfiguration.Builder setMaxBuffersReadAhead(int maxBuffersReadAhead) {
            this.maxBuffersReadAhead = maxBuffersReadAhead;
            return this;
        }

        public TieredStoreConfiguration.Builder setBufferRequestTimeout(
                Duration bufferRequestTimeout) {
            this.bufferRequestTimeout = bufferRequestTimeout;
            return this;
        }

        public TieredStoreConfiguration.Builder setSelectiveStrategySpillThreshold(
                float selectiveStrategySpillThreshold) {
            this.selectiveStrategySpillThreshold = selectiveStrategySpillThreshold;
            return this;
        }

        public TieredStoreConfiguration.Builder setSelectiveStrategySpillBufferRatio(
                float selectiveStrategySpillBufferRatio) {
            this.selectiveStrategySpillBufferRatio = selectiveStrategySpillBufferRatio;
            return this;
        }

        public TieredStoreConfiguration.Builder setFullStrategyNumBuffersTriggerSpillingRatio(
                float fullStrategyNumBuffersTriggerSpillingRatio) {
            this.fullStrategyNumBuffersTriggerSpillingRatio =
                    fullStrategyNumBuffersTriggerSpillingRatio;
            return this;
        }

        public TieredStoreConfiguration.Builder setFullStrategyReleaseThreshold(
                float fullStrategyReleaseThreshold) {
            this.fullStrategyReleaseThreshold = fullStrategyReleaseThreshold;
            return this;
        }

        public TieredStoreConfiguration.Builder setFullStrategyReleaseBufferRatio(
                float fullStrategyReleaseBufferRatio) {
            this.fullStrategyReleaseBufferRatio = fullStrategyReleaseBufferRatio;
            return this;
        }

        public TieredStoreConfiguration.Builder setTieredStoreBufferInMemoryRatio(
                float tieredStoreBufferInMemoryRatio) {
            this.tieredStoreBufferInMemoryRatio = tieredStoreBufferInMemoryRatio;
            return this;
        }

        public TieredStoreConfiguration.Builder setTieredStoreFlushBufferRatio(
                float tieredStoreFlushBufferRatio) {
            this.tieredStoreFlushBufferRatio = tieredStoreFlushBufferRatio;
            return this;
        }

        public TieredStoreConfiguration.Builder setTieredStoreTriggerFlushRatio(
                float tieredStoreTriggerFlushRatio) {
            this.tieredStoreTriggerFlushRatio = tieredStoreTriggerFlushRatio;
            return this;
        }

        public TieredStoreConfiguration.Builder setNumBuffersTriggerFlushRatio(
                float numBuffersTriggerFlushRatio) {
            this.numBuffersTriggerFlushRatio = numBuffersTriggerFlushRatio;
            return this;
        }

        public TieredStoreConfiguration.Builder setBufferPoolSizeCheckIntervalMs(
                long bufferPoolSizeCheckIntervalMs) {
            this.bufferPoolSizeCheckIntervalMs = bufferPoolSizeCheckIntervalMs;
            return this;
        }

        public TieredStoreConfiguration.Builder setBaseDfsHomePath(String baseDfsHomePath) {
            this.baseDfsHomePath = baseDfsHomePath;
            return this;
        }

        public TieredStoreConfiguration.Builder setConfiguredNetworkBuffersPerChannel(
                int configuredNetworkBuffersPerChannel) {
            this.configuredNetworkBuffersPerChannel = configuredNetworkBuffersPerChannel;
            return this;
        }

        public TieredStoreConfiguration.Builder setTierTypes(
                String configuredStoreTiers, ResultPartitionType partitionType) {
            this.tierTypes = getConfiguredTierTypes(configuredStoreTiers, partitionType);
            this.upstreamTierTypes =
                    getConfiguredUpstreamTierTypes(configuredStoreTiers, partitionType);
            this.remoteTierTypes =
                    getConfiguredRemoteTierTypes(configuredStoreTiers, partitionType);
            System.out.println();
            checkState(tierTypes.length == upstreamTierTypes.length + remoteTierTypes.length);
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

        public TieredStoreConfiguration build() {
            return new TieredStoreConfiguration(
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
                    remoteTierTypes);
        }
    }
}