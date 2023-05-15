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
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierMemorySpec;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.local.disk.DiskTierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.local.memory.MemoryTierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.remote.RemoteTierFactory;

import java.time.Duration;
import java.util.ArrayList;
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

    private static final int DEFAULT_MAX_BUFFERS_READ_AHEAD = 5;

    private static final Duration DEFAULT_BUFFER_REQUEST_TIMEOUT = Duration.ofMinutes(5);

    private static final float DEFAULT_TIERED_STORE_BUFFER_IN_MEMORY_RATIO = 0.4f;

    private static final float DEFAULT_TIERED_STORE_FLUSH_BUFFER_RATIO = 0.2f;

    private static final float DEFAULT_TIERED_STORE_TRIGGER_FLUSH_RATIO = 0.8f;

    private static final float DEFAULT_NUM_BUFFERS_TRIGGER_FLUSH_RATIO = 0.6f;

    private static final long DEFAULT_BUFFER_POLL_SIZE_CHECK_INTERVAL_MS = 1000;

    private static final TierType[] MEMORY_ONLY_TIER_TYPE = new TierType[] {TierType.IN_MEM};

    // TODO, remove this DEFAULT_MEMORY_DISK_TIER_TYPES
    private static final TierType[] DEFAULT_MEMORY_DISK_TIER_TYPES =
            new TierType[] {TierType.IN_MEM, TierType.IN_DISK};

    // TODO, remove this DEFAULT_MEMORY_DISK_REMOTE_TIER_TYPES
    private static final TierType[] DEFAULT_MEMORY_DISK_REMOTE_TIER_TYPES =
            new TierType[] {TierType.IN_MEM, TierType.IN_DISK, TierType.IN_REMOTE};

    private static final TierFactory[] DEFAULT_MEMORY_DISK_TIER_FACTORIES =
            new TierFactory[] {new MemoryTierFactory(), new DiskTierFactory()};

    private static final TierFactory[] DEFAULT_MEMORY_DISK_REMOTE_TIER_FACTORIES =
            new TierFactory[] {
                new MemoryTierFactory(), new DiskTierFactory(), new RemoteTierFactory()
            };

    private static final TierMemorySpec[] DEFAULT_MEMORY_DISK_TIER_MEMORY_SPECS =
            new TierMemorySpec[] {new TierMemorySpec(100, false), new TierMemorySpec(1, true)};

    private static final TierMemorySpec[] DEFAULT_MEMORY_DISK_REMOTE_TIER_MEMORY_SPECS =
            new TierMemorySpec[] {
                new TierMemorySpec(100, false),
                new TierMemorySpec(1, true),
                new TierMemorySpec(1, true)
            };

    private static final int[] DEFAULT_MEMORY_DISK_EXCLUSIVE_BUFFERS = new int[] {100, 1, 1};

    private static final int[] DEFAULT_MEMORY_DISK_REMOTE_EXCLUSIVE_BUFFERS =
            new int[] {100, 1, 1, 1};

    private static final boolean[] DEFAULT_MEMORY_DISK_MEMORY_RELEASABLE =
            new boolean[] {false, true, true};

    private static final boolean[] DEFAULT_MEMORY_DISK_MEMORY_REMOTE_RELEASABLE =
            new boolean[] {false, true, true, true};

    private final int maxBuffersReadAhead;

    private final Duration bufferRequestTimeout;

    private final int maxRequestedBuffers;

    // For Memory Tier
    private final int configuredNetworkBuffersPerChannel;

    private final float tieredStoreBufferInMemoryRatio;

    private final float tieredStoreFlushBufferRatio;

    private final float tieredStoreTriggerFlushRatio;

    private final float numBuffersTriggerFlushRatio;

    private final long bufferPoolSizeCheckIntervalMs;

    private final String baseDfsHomePath;

    private int[] tierExclusiveBuffers;

    private boolean[] tierMemoryReleasable;

    private final List<IndexedTierConfSpec> indexedTierConfSpecs;

    private final TierFactory[] tierFactories;

    private TieredStorageConfiguration(
            int maxBuffersReadAhead,
            Duration bufferRequestTimeout,
            int maxRequestedBuffers,
            float tieredStoreBufferInMemoryRatio,
            float tieredStoreFlushBufferRatio,
            float tieredStoreTriggerFlushRatio,
            float numBuffersTriggerFlushRatio,
            long bufferPoolSizeCheckIntervalMs,
            String baseDfsHomePath,
            int configuredNetworkBuffersPerChannel,
            int[] tierExclusiveBuffers,
            boolean[] tierMemoryReleasable,
            List<IndexedTierConfSpec> indexedTierConfSpecs,
            TierFactory[] tierFactories,
            TierMemorySpec[] tierMemorySpecs) {
        this.maxBuffersReadAhead = maxBuffersReadAhead;
        this.bufferRequestTimeout = bufferRequestTimeout;
        this.maxRequestedBuffers = maxRequestedBuffers;
        this.tieredStoreBufferInMemoryRatio = tieredStoreBufferInMemoryRatio;
        this.tieredStoreFlushBufferRatio = tieredStoreFlushBufferRatio;
        this.tieredStoreTriggerFlushRatio = tieredStoreTriggerFlushRatio;
        this.numBuffersTriggerFlushRatio = numBuffersTriggerFlushRatio;
        this.bufferPoolSizeCheckIntervalMs = bufferPoolSizeCheckIntervalMs;
        this.baseDfsHomePath = baseDfsHomePath;
        this.configuredNetworkBuffersPerChannel = configuredNetworkBuffersPerChannel;
        this.tierExclusiveBuffers = tierExclusiveBuffers;
        this.tierMemoryReleasable = tierMemoryReleasable;
        this.indexedTierConfSpecs = indexedTierConfSpecs;
        this.tierFactories = tierFactories;
    }

    public static TieredStorageConfiguration.Builder builder() {
        return new TieredStorageConfiguration.Builder();
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

    public List<TierFactory> getTierFactories() {
        return Arrays.asList(tierFactories);
    }

    public int[] getTierExclusiveBuffers() {
        return tierExclusiveBuffers;
    }

    public boolean[] getTierMemoryReleasable() {
        return tierMemoryReleasable;
    }

    public List<IndexedTierConfSpec> getIndexedTierConfSpecs() {
        return indexedTierConfSpecs;
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

        private String baseDfsHomePath = null;

        private int configuredNetworkBuffersPerChannel;

        private TierType[] tierTypes;

        private TierFactory[] tierFactories;

        private TierMemorySpec[] tierMemorySpecs;

        private int[] tierExclusiveBuffers;

        private boolean[] tierMemoryReleasable;

        private int numSubpartitions;

        private List<TierConfSpec> tierConfSpecs;

        private final List<IndexedTierConfSpec> indexedTierConfSpecs = new ArrayList<>();

        private int numBuffersPerRequest;

        private Builder() {}

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

        /** Only for test. This method will be removed in the production code. */
        public TieredStorageConfiguration.Builder setTierTypes(
                String configuredStoreTiers, ResultPartitionType partitionType) {
            tierTypes = getConfiguredTierTypes(configuredStoreTiers, partitionType);
            tierFactories = getTierFactories(tierTypes);
            // After the method is replaced, these generation of tierConfSpecs is useless.
            tierConfSpecs = new ArrayList<>();
            indexedTierConfSpecs.clear();
            tierMemorySpecs = new TierMemorySpec[tierTypes.length];
            tierExclusiveBuffers = new int[tierTypes.length];
            tierMemoryReleasable = new boolean[tierTypes.length];

            for (int i = 0; i < tierTypes.length; i++) {
                int numExclusive = HYBRID_SHUFFLE_TIER_EXCLUSIVE_BUFFERS.get(tierTypes[i]);
                TierConfSpec tierConfSpec =
                        new TierConfSpec(numExclusive, tierTypes[i] != TierType.IN_MEM);
                tierConfSpecs.add(tierConfSpec);
                indexedTierConfSpecs.add(new IndexedTierConfSpec(i, tierConfSpec));
                tierExclusiveBuffers[i] = tierTypes[i] == TierType.IN_MEM ? 100 : 1;
                tierMemoryReleasable[i] = tierTypes[i] != TierType.IN_MEM;
                tierMemorySpecs[i] =
                        new TierMemorySpec(numExclusive, tierTypes[i] != TierType.IN_MEM);
            }
            return this;
        }

        /** Only for test. This method will be removed in the production code. */
        public TieredStorageConfiguration.Builder setTierTypes(String configuredStoreTiers) {
            tierTypes = getConfiguredTierTypes(configuredStoreTiers);
            tierFactories = getTierFactories(tierTypes);
            // After the method is replaced, these generation of tierConfSpecs is useless.
            tierConfSpecs = new ArrayList<>();
            indexedTierConfSpecs.clear();
            for (int i = 0; i < tierTypes.length; i++) {
                int numExclusive = HYBRID_SHUFFLE_TIER_EXCLUSIVE_BUFFERS.get(tierTypes[i]);
                TierConfSpec tierConfSpec =
                        new TierConfSpec(numExclusive, tierTypes[i] != TierType.IN_MEM);
                tierConfSpecs.add(tierConfSpec);
                indexedTierConfSpecs.add(new IndexedTierConfSpec(i, tierConfSpec));
            }
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
            if (tierTypes == null) {
                this.tierTypes = getDefaultTierTypes(baseDfsHomePath);
                this.tierMemorySpecs = getDefaultTierMemorySpecs(baseDfsHomePath);
            }
            if (tierFactories == null) {
                this.tierFactories = getDefaultTierFactories(baseDfsHomePath);
            }
            if (tierExclusiveBuffers == null) {
                this.tierExclusiveBuffers = getDefaultExclusiveBuffers(baseDfsHomePath);
            }
            if (tierMemoryReleasable == null) {
                this.tierMemoryReleasable = getDefaultMemoryReleasable(baseDfsHomePath);
            }

            return new TieredStorageConfiguration(
                    maxBuffersReadAhead,
                    bufferRequestTimeout,
                    Math.max(2 * numBuffersPerRequest, numSubpartitions),
                    tieredStoreBufferInMemoryRatio,
                    tieredStoreFlushBufferRatio,
                    tieredStoreTriggerFlushRatio,
                    numBuffersTriggerFlushRatio,
                    bufferPoolSizeCheckIntervalMs,
                    baseDfsHomePath,
                    configuredNetworkBuffersPerChannel,
                    tierExclusiveBuffers,
                    tierMemoryReleasable,
                    indexedTierConfSpecs,
                    tierFactories,
                    tierMemorySpecs);
        }
    }

    // Only for tests
    private static TierFactory[] getTierFactories(TierType[] tierTypes) {
        TierFactory[] tierFactories = new TierFactory[tierTypes.length];
        for (int i = 0; i < tierTypes.length; i++) {
            tierFactories[i] = createTierFactory(tierTypes[i]);
        }
        return tierFactories;
    }

    private static TierFactory createTierFactory(TierType tierType) {
        switch (tierType) {
            case IN_MEM:
                return new MemoryTierFactory();
            case IN_DISK:
                return new DiskTierFactory();
            case IN_REMOTE:
                return new RemoteTierFactory();
            default:
                throw new IllegalArgumentException("Illegal tier type " + tierType);
        }
    }

    private static TierType[] getDefaultTierTypes(String baseDfsHomePath) {
        return baseDfsHomePath == null
                ? DEFAULT_MEMORY_DISK_TIER_TYPES
                : DEFAULT_MEMORY_DISK_REMOTE_TIER_TYPES;
    }

    private static TierMemorySpec[] getDefaultTierMemorySpecs(String baseDfsHomePath) {
        return baseDfsHomePath == null
                ? DEFAULT_MEMORY_DISK_TIER_MEMORY_SPECS
                : DEFAULT_MEMORY_DISK_REMOTE_TIER_MEMORY_SPECS;
    }

    private static TierFactory[] getDefaultTierFactories(String baseDfsHomePath) {
        return baseDfsHomePath == null
                ? DEFAULT_MEMORY_DISK_TIER_FACTORIES
                : DEFAULT_MEMORY_DISK_REMOTE_TIER_FACTORIES;
    }

    private static int[] getDefaultExclusiveBuffers(String baseDfsHomePath) {
        return baseDfsHomePath == null
                ? DEFAULT_MEMORY_DISK_EXCLUSIVE_BUFFERS
                : DEFAULT_MEMORY_DISK_REMOTE_EXCLUSIVE_BUFFERS;
    }

    private static boolean[] getDefaultMemoryReleasable(String baseDfsHomePath) {
        return baseDfsHomePath == null
                ? DEFAULT_MEMORY_DISK_MEMORY_RELEASABLE
                : DEFAULT_MEMORY_DISK_MEMORY_REMOTE_RELEASABLE;
    }
}
