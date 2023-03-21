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

package org.apache.flink.test.runtime;

import org.apache.flink.api.common.BatchShuffleMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.flink.configuration.NettyShuffleEnvironmentOptions.TIERED_STORE_TIERS;

/** Tests for TieredStore Shuffle. */
class TieredStoreShuffleITCase extends TieredStoreBatchShuffleITCaseBase {

    // ------------------------------------
    //        For Memory Tier
    // ------------------------------------

    public final TemporaryFolder tmp = new TemporaryFolder();

    @BeforeEach
    public void before() throws Exception {
        tmp.create();
    }

    @AfterEach
    public void after() {
        tmp.delete();
    }

    @Test
    void testTieredStoreMemory() throws Exception {
        final int numRecordsToSend = 1000;
        Configuration configuration = getConfiguration();
        configuration.set(
                ExecutionOptions.BATCH_SHUFFLE_MODE,
                BatchShuffleMode.ALL_EXCHANGES_HYBRID_SELECTIVE);
        configuration.set(
                NettyShuffleEnvironmentOptions.NETWORK_HYBRID_SHUFFLE_ENABLE_TIERED_STORE, true);
        configuration.set(TIERED_STORE_TIERS, "MEMORY");
        JobGraph jobGraph = createJobGraph(numRecordsToSend, false, configuration);
        executeJob(jobGraph, configuration, numRecordsToSend);
    }

    @Test
    @Disabled("Broadcast is not supported when only memory tier is exist")
    void testTieredStoreMemoryBroadcast() throws Exception {
        final int numRecordsToSend = 1000;
        Configuration configuration = getConfiguration();
        configuration.set(
                ExecutionOptions.BATCH_SHUFFLE_MODE,
                BatchShuffleMode.ALL_EXCHANGES_HYBRID_SELECTIVE);
        configuration.set(
                NettyShuffleEnvironmentOptions.NETWORK_HYBRID_SHUFFLE_ENABLE_TIERED_STORE, true);
        configuration.set(TIERED_STORE_TIERS, "MEMORY");
        JobGraph jobGraph = createJobGraph(numRecordsToSend, false, configuration, true);
        executeJob(jobGraph, configuration, numRecordsToSend, true);
    }

    @Test
    void testTieredStoreMemoryRestart() throws Exception {
        final int numRecordsToSend = 1000;
        Configuration configuration = getConfiguration();
        configuration.set(
                ExecutionOptions.BATCH_SHUFFLE_MODE,
                BatchShuffleMode.ALL_EXCHANGES_HYBRID_SELECTIVE);
        configuration.set(
                NettyShuffleEnvironmentOptions.NETWORK_HYBRID_SHUFFLE_ENABLE_TIERED_STORE, true);
        configuration.set(TIERED_STORE_TIERS, "MEMORY");
        JobGraph jobGraph = createJobGraph(numRecordsToSend, true, configuration);
        executeJob(jobGraph, configuration, numRecordsToSend);
    }

    // ------------------------------------
    //        For Local Tier
    // ------------------------------------

    @Test
    void testTieredStoreLocal() throws Exception {
        final int numRecordsToSend = 10000;
        Configuration configuration = getConfiguration();
        configuration.set(
                ExecutionOptions.BATCH_SHUFFLE_MODE,
                BatchShuffleMode.ALL_EXCHANGES_HYBRID_SELECTIVE);
        configuration.set(
                NettyShuffleEnvironmentOptions.NETWORK_HYBRID_SHUFFLE_ENABLE_TIERED_STORE, true);
        configuration.set(TIERED_STORE_TIERS, "LOCAL");
        JobGraph jobGraph = createJobGraph(numRecordsToSend, false, configuration);
        executeJob(jobGraph, configuration, numRecordsToSend);
    }

    @Test
    void testTieredStoreLocalBroadcast() throws Exception {
        final int numRecordsToSend = 1000;
        Configuration configuration = getConfiguration();
        configuration.set(
                ExecutionOptions.BATCH_SHUFFLE_MODE,
                BatchShuffleMode.ALL_EXCHANGES_HYBRID_SELECTIVE);
        configuration.set(
                NettyShuffleEnvironmentOptions.NETWORK_HYBRID_SHUFFLE_ENABLE_TIERED_STORE, true);
        configuration.set(TIERED_STORE_TIERS, "LOCAL");
        JobGraph jobGraph = createJobGraph(numRecordsToSend, false, configuration, true);
        executeJob(jobGraph, configuration, numRecordsToSend, true);
    }

    @Test
    void testTieredStoreLocalRestart() throws Exception {
        final int numRecordsToSend = 1000;
        Configuration configuration = getConfiguration();
        configuration.set(
                ExecutionOptions.BATCH_SHUFFLE_MODE,
                BatchShuffleMode.ALL_EXCHANGES_HYBRID_SELECTIVE);
        configuration.set(
                NettyShuffleEnvironmentOptions.NETWORK_HYBRID_SHUFFLE_ENABLE_TIERED_STORE, true);
        configuration.set(TIERED_STORE_TIERS, "LOCAL");
        JobGraph jobGraph = createJobGraph(numRecordsToSend, true, configuration);
        executeJob(jobGraph, configuration, numRecordsToSend);
    }

    // ------------------------------------
    //        For Dfs Tier
    // ------------------------------------

    @Test
    void testTieredStoreDfs() throws Exception {
        final int numRecordsToSend = 10;
        Configuration configuration = getConfiguration();
        configuration.set(
                ExecutionOptions.BATCH_SHUFFLE_MODE,
                BatchShuffleMode.ALL_EXCHANGES_HYBRID_SELECTIVE);
        configuration.set(
                NettyShuffleEnvironmentOptions.NETWORK_HYBRID_SHUFFLE_ENABLE_TIERED_STORE, true);
        setupDfsConfigurations(configuration);
        configuration.set(TIERED_STORE_TIERS, "DFS");
        JobGraph jobGraph = createJobGraph(numRecordsToSend, false, false, configuration, 1);
        executeJob(jobGraph, configuration, numRecordsToSend);
    }

    @Test
    void testTieredStoreDfsBroadcast() throws Exception {
        final int numRecordsToSend = 10;
        Configuration configuration = getConfiguration();
        configuration.set(
                ExecutionOptions.BATCH_SHUFFLE_MODE,
                BatchShuffleMode.ALL_EXCHANGES_HYBRID_SELECTIVE);
        configuration.set(
                NettyShuffleEnvironmentOptions.NETWORK_HYBRID_SHUFFLE_ENABLE_TIERED_STORE, true);
        setupDfsConfigurations(configuration);
        configuration.set(TIERED_STORE_TIERS, "DFS");
        JobGraph jobGraph = createJobGraph(numRecordsToSend, false, false, configuration, 4, true);
        executeJob(jobGraph, configuration, numRecordsToSend, true);
    }

    @Test
    void testTieredStoreDfsRestart() throws Exception {
        final int numRecordsToSend = 10;
        Configuration configuration = getConfiguration();
        configuration.set(
                ExecutionOptions.BATCH_SHUFFLE_MODE,
                BatchShuffleMode.ALL_EXCHANGES_HYBRID_SELECTIVE);
        configuration.set(
                NettyShuffleEnvironmentOptions.NETWORK_HYBRID_SHUFFLE_ENABLE_TIERED_STORE, true);
        setupDfsConfigurations(configuration);
        configuration.set(TIERED_STORE_TIERS, "DFS");
        JobGraph jobGraph = createJobGraph(numRecordsToSend, true, false, configuration, 4);
        executeJob(jobGraph, configuration, numRecordsToSend);
    }

    // ------------------------------------
    //        For Memory,Local Tier
    // ------------------------------------

    @Test
    void testTieredStoreMemoryLocalSelective() throws Exception {
        final int numRecordsToSend = 10000;
        Configuration configuration = getConfiguration();
        configuration.set(
                ExecutionOptions.BATCH_SHUFFLE_MODE,
                BatchShuffleMode.ALL_EXCHANGES_HYBRID_SELECTIVE);
        configuration.set(
                NettyShuffleEnvironmentOptions.NETWORK_HYBRID_SHUFFLE_ENABLE_TIERED_STORE, true);
        configuration.set(TIERED_STORE_TIERS, "MEMORY_LOCAL");
        JobGraph jobGraph = createJobGraph(numRecordsToSend, false, configuration);
        executeJob(jobGraph, configuration, numRecordsToSend);
    }

    @Test
    void testTieredStoreMemoryLocalSelectiveBroadcast() throws Exception {
        final int numRecordsToSend = 1000;
        Configuration configuration = getConfiguration();
        configuration.set(
                ExecutionOptions.BATCH_SHUFFLE_MODE,
                BatchShuffleMode.ALL_EXCHANGES_HYBRID_SELECTIVE);
        configuration.set(
                NettyShuffleEnvironmentOptions.NETWORK_HYBRID_SHUFFLE_ENABLE_TIERED_STORE, true);
        configuration.set(TIERED_STORE_TIERS, "MEMORY_LOCAL");
        JobGraph jobGraph = createJobGraph(numRecordsToSend, false, configuration, true);
        executeJob(jobGraph, configuration, numRecordsToSend, true);
    }

    @Test
    void testTieredStoreMemoryLocalSelectiveRestart() throws Exception {
        final int numRecordsToSend = 1000;
        Configuration configuration = getConfiguration();
        configuration.set(
                ExecutionOptions.BATCH_SHUFFLE_MODE,
                BatchShuffleMode.ALL_EXCHANGES_HYBRID_SELECTIVE);
        configuration.set(
                NettyShuffleEnvironmentOptions.NETWORK_HYBRID_SHUFFLE_ENABLE_TIERED_STORE, true);
        configuration.set(TIERED_STORE_TIERS, "MEMORY_LOCAL");
        JobGraph jobGraph = createJobGraph(numRecordsToSend, true, configuration);
        executeJob(jobGraph, configuration, numRecordsToSend);
    }

    @Test
    void testTieredStoreMemoryLocalFull() throws Exception {
        final int numRecordsToSend = 10000;
        Configuration configuration = getConfiguration();
        configuration.set(
                ExecutionOptions.BATCH_SHUFFLE_MODE, BatchShuffleMode.ALL_EXCHANGES_HYBRID_FULL);
        configuration.set(
                NettyShuffleEnvironmentOptions.NETWORK_HYBRID_SHUFFLE_ENABLE_TIERED_STORE, true);
        configuration.set(TIERED_STORE_TIERS, "MEMORY_LOCAL");
        JobGraph jobGraph = createJobGraph(numRecordsToSend, false, configuration);
        executeJob(jobGraph, configuration, numRecordsToSend);
    }

    @Test
    void testTieredStoreMemoryLocalFullBroadcast() throws Exception {
        final int numRecordsToSend = 1000;
        Configuration configuration = getConfiguration();
        configuration.set(
                ExecutionOptions.BATCH_SHUFFLE_MODE, BatchShuffleMode.ALL_EXCHANGES_HYBRID_FULL);
        configuration.set(
                NettyShuffleEnvironmentOptions.NETWORK_HYBRID_SHUFFLE_ENABLE_TIERED_STORE, true);
        configuration.set(TIERED_STORE_TIERS, "MEMORY_LOCAL");
        JobGraph jobGraph = createJobGraph(numRecordsToSend, false, configuration, true);
        executeJob(jobGraph, configuration, numRecordsToSend, true);
    }

    @Test
    void testTieredStoreMemoryLocalFullRestart() throws Exception {
        final int numRecordsToSend = 1000;
        Configuration configuration = getConfiguration();
        configuration.set(
                ExecutionOptions.BATCH_SHUFFLE_MODE, BatchShuffleMode.ALL_EXCHANGES_HYBRID_FULL);
        configuration.set(
                NettyShuffleEnvironmentOptions.NETWORK_HYBRID_SHUFFLE_ENABLE_TIERED_STORE, true);
        configuration.set(TIERED_STORE_TIERS, "MEMORY_LOCAL");
        JobGraph jobGraph = createJobGraph(numRecordsToSend, true, configuration);
        executeJob(jobGraph, configuration, numRecordsToSend);
    }

    // ------------------------------------
    //        For Memory,Dfs Tier
    // ------------------------------------

    @Test
    void testTieredStoreMemoryDfsSelective() throws Exception {
        final int numRecordsToSend = 10000;
        Configuration configuration = getConfiguration();
        configuration.set(
                ExecutionOptions.BATCH_SHUFFLE_MODE,
                BatchShuffleMode.ALL_EXCHANGES_HYBRID_SELECTIVE);
        configuration.set(
                NettyShuffleEnvironmentOptions.NETWORK_HYBRID_SHUFFLE_ENABLE_TIERED_STORE, true);
        configuration.set(TIERED_STORE_TIERS, "MEMORY_DFS");
        setupDfsConfigurations(configuration);
        JobGraph jobGraph = createJobGraph(numRecordsToSend, false, configuration);
        executeJob(jobGraph, configuration, numRecordsToSend);
    }

    @Test
    void testTieredStoreMemoryDfsSelectiveBroadcast() throws Exception {
        final int numRecordsToSend = 1000;
        Configuration configuration = getConfiguration();
        configuration.set(
                ExecutionOptions.BATCH_SHUFFLE_MODE,
                BatchShuffleMode.ALL_EXCHANGES_HYBRID_SELECTIVE);
        configuration.set(
                NettyShuffleEnvironmentOptions.NETWORK_HYBRID_SHUFFLE_ENABLE_TIERED_STORE, true);
        configuration.set(TIERED_STORE_TIERS, "MEMORY_DFS");
        setupDfsConfigurations(configuration);
        JobGraph jobGraph = createJobGraph(numRecordsToSend, false, configuration, true);
        executeJob(jobGraph, configuration, numRecordsToSend, true);
    }

    @Test
    void testTieredStoreMemoryDfsSelectiveRestart() throws Exception {
        final int numRecordsToSend = 1000;
        Configuration configuration = getConfiguration();
        configuration.set(
                ExecutionOptions.BATCH_SHUFFLE_MODE,
                BatchShuffleMode.ALL_EXCHANGES_HYBRID_SELECTIVE);
        configuration.set(
                NettyShuffleEnvironmentOptions.NETWORK_HYBRID_SHUFFLE_ENABLE_TIERED_STORE, true);
        configuration.set(TIERED_STORE_TIERS, "MEMORY_DFS");
        setupDfsConfigurations(configuration);
        JobGraph jobGraph = createJobGraph(numRecordsToSend, true, configuration);
        executeJob(jobGraph, configuration, numRecordsToSend);
    }

    // ------------------------------------
    //        For Memory,Local,Dfs Tier
    // ------------------------------------

    @Test
    void testTieredStoreMemoryLocalDfsSelective() throws Exception {
        final int numRecordsToSend = 10000;
        Configuration configuration = getConfiguration();
        configuration.set(
                ExecutionOptions.BATCH_SHUFFLE_MODE,
                BatchShuffleMode.ALL_EXCHANGES_HYBRID_SELECTIVE);
        configuration.set(
                NettyShuffleEnvironmentOptions.NETWORK_HYBRID_SHUFFLE_ENABLE_TIERED_STORE, true);
        configuration.set(TIERED_STORE_TIERS, "MEMORY_LOCAL_DFS");
        setupDfsConfigurations(configuration);
        JobGraph jobGraph = createJobGraph(numRecordsToSend, false, configuration);
        executeJob(jobGraph, configuration, numRecordsToSend);
    }

    @Test
    void testTieredStoreMemoryLocalDfsSelectiveBroadcast() throws Exception {
        final int numRecordsToSend = 1000;
        Configuration configuration = getConfiguration();
        configuration.set(
                ExecutionOptions.BATCH_SHUFFLE_MODE,
                BatchShuffleMode.ALL_EXCHANGES_HYBRID_SELECTIVE);
        configuration.set(
                NettyShuffleEnvironmentOptions.NETWORK_HYBRID_SHUFFLE_ENABLE_TIERED_STORE, true);
        configuration.set(TIERED_STORE_TIERS, "MEMORY_LOCAL_DFS");
        setupDfsConfigurations(configuration);
        JobGraph jobGraph = createJobGraph(numRecordsToSend, false, configuration, true);
        executeJob(jobGraph, configuration, numRecordsToSend, true);
    }

    @Test
    void testTieredStoreMemoryLocalDfsSelectiveRestart() throws Exception {
        final int numRecordsToSend = 1000;
        Configuration configuration = getConfiguration();
        configuration.set(
                ExecutionOptions.BATCH_SHUFFLE_MODE,
                BatchShuffleMode.ALL_EXCHANGES_HYBRID_SELECTIVE);
        configuration.set(
                NettyShuffleEnvironmentOptions.NETWORK_HYBRID_SHUFFLE_ENABLE_TIERED_STORE, true);
        configuration.set(TIERED_STORE_TIERS, "MEMORY_LOCAL_DFS");
        setupDfsConfigurations(configuration);
        JobGraph jobGraph = createJobGraph(numRecordsToSend, true, configuration);
        executeJob(jobGraph, configuration, numRecordsToSend);
    }

    // ------------------------------------
    //        For Local,Dfs Tier
    // ------------------------------------

    @Test
    void testTieredStoreLocalDfsFull() throws Exception {
        final int numRecordsToSend = 10000;
        Configuration configuration = getConfiguration();
        configuration.set(
                ExecutionOptions.BATCH_SHUFFLE_MODE, BatchShuffleMode.ALL_EXCHANGES_HYBRID_FULL);
        configuration.set(
                NettyShuffleEnvironmentOptions.NETWORK_HYBRID_SHUFFLE_ENABLE_TIERED_STORE, true);
        configuration.set(TIERED_STORE_TIERS, "LOCAL_DFS");
        setupDfsConfigurations(configuration);
        JobGraph jobGraph = createJobGraph(numRecordsToSend, false, configuration);
        executeJob(jobGraph, configuration, numRecordsToSend);
    }

    @Test
    void testTieredStoreLocalDfsFullBroadcast() throws Exception {
        final int numRecordsToSend = 1000;
        Configuration configuration = getConfiguration();
        configuration.set(
                ExecutionOptions.BATCH_SHUFFLE_MODE, BatchShuffleMode.ALL_EXCHANGES_HYBRID_FULL);
        configuration.set(
                NettyShuffleEnvironmentOptions.NETWORK_HYBRID_SHUFFLE_ENABLE_TIERED_STORE, true);
        configuration.set(TIERED_STORE_TIERS, "LOCAL_DFS");
        setupDfsConfigurations(configuration);
        JobGraph jobGraph = createJobGraph(numRecordsToSend, false, configuration, true);
        executeJob(jobGraph, configuration, numRecordsToSend, true);
    }

    @Test
    void testTieredStoreLocalDfsFullRestart() throws Exception {
        final int numRecordsToSend = 1000;
        Configuration configuration = getConfiguration();
        configuration.set(
                ExecutionOptions.BATCH_SHUFFLE_MODE, BatchShuffleMode.ALL_EXCHANGES_HYBRID_FULL);
        configuration.set(
                NettyShuffleEnvironmentOptions.NETWORK_HYBRID_SHUFFLE_ENABLE_TIERED_STORE, true);
        configuration.set(TIERED_STORE_TIERS, "LOCAL_DFS");
        setupDfsConfigurations(configuration);
        JobGraph jobGraph = createJobGraph(numRecordsToSend, true, configuration);
        executeJob(jobGraph, configuration, numRecordsToSend);
    }

    private void setupDfsConfigurations(Configuration configuration) {
        System.out.println("----------------remote path : " + tmp.getRoot().getAbsolutePath());
        configuration.set(
                NettyShuffleEnvironmentOptions.NETWORK_HYBRID_SHUFFLE_REMOTE_STORAGE_BASE_HOME_PATH,
                tmp.getRoot().getAbsolutePath());
    }
}
