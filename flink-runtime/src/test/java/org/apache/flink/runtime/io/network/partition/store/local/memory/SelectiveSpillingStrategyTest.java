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

package org.apache.flink.runtime.io.network.partition.store.local.memory;

import org.apache.flink.runtime.io.network.partition.store.TieredStoreConfiguration;
import org.apache.flink.runtime.io.network.partition.store.common.BufferIndexAndChannel;
import org.apache.flink.runtime.io.network.partition.store.tier.local.file.SelectiveSpillingStrategy;
import org.apache.flink.runtime.io.network.partition.store.tier.local.file.TsSpillingStrategy.Decision;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.runtime.io.network.partition.store.TieredStoreTestUtils.createBufferIndexAndChannelsList;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SelectiveSpillingStrategy}. */
class SelectiveSpillingStrategyTest {
    public static final int NUM_SUBPARTITIONS = 3;

    public static final float SELECTIVE_SPILL_THRESHOLD = 0.7f;

    public static final float SELECTIVE_SPILL_BUFFER_RATIO = 0.3f;

    private final SelectiveSpillingStrategy spillStrategy =
            new SelectiveSpillingStrategy(
                    TieredStoreConfiguration.builder(NUM_SUBPARTITIONS, 1)
                            .setSelectiveStrategySpillThreshold(SELECTIVE_SPILL_THRESHOLD)
                            .setSelectiveStrategySpillBufferRatio(SELECTIVE_SPILL_BUFFER_RATIO)
                            .build());

    @Test
    void testOnBufferFinished() {
        Optional<Decision> finishedDecision = spillStrategy.onBufferFinished(5, 10);
        assertThat(finishedDecision).hasValue(Decision.NO_ACTION);
    }

    @Test
    void testOnBufferConsumed() {
        BufferIndexAndChannel bufferIndexAndChannel = new BufferIndexAndChannel(0, 0);
        Optional<Decision> consumedDecision = spillStrategy.onBufferConsumed(bufferIndexAndChannel);
        assertThat(consumedDecision)
                .hasValueSatisfying(
                        (decision -> {
                            assertThat(decision.getBufferToRelease())
                                    .hasSize(1)
                                    .hasEntrySatisfying(
                                            0,
                                            (list) ->
                                                    assertThat(list)
                                                            .containsExactly(
                                                                    bufferIndexAndChannel));
                            assertThat(decision.getBufferToSpill()).isEmpty();
                        }));
    }

    @Test
    void testOnResultPartitionClosed() {
        final int subpartition1 = 0;
        final int subpartition2 = 1;

        List<BufferIndexAndChannel> subpartitionBuffer1 =
                createBufferIndexAndChannelsList(subpartition1, 0, 1, 2, 3);
        List<BufferIndexAndChannel> subpartitionBuffer2 =
                createBufferIndexAndChannelsList(subpartition2, 0, 1, 2);
        TestingSpillingInfoProvider spillInfoProvider =
                TestingSpillingInfoProvider.builder()
                        .setGetNumSubpartitionsSupplier(() -> 2)
                        .addSubpartitionBuffers(subpartition1, subpartitionBuffer1)
                        .addSubpartitionBuffers(subpartition2, subpartitionBuffer2)
                        .addSpillBuffers(subpartition1, Arrays.asList(2, 3))
                        .addConsumedBuffers(subpartition1, Collections.singletonList(0))
                        .addSpillBuffers(subpartition2, Collections.singletonList(2))
                        .build();

        Decision decision = spillStrategy.onResultPartitionClosed(spillInfoProvider);

        Map<Integer, List<BufferIndexAndChannel>> expectedToSpillBuffers = new HashMap<>();
        expectedToSpillBuffers.put(subpartition1, subpartitionBuffer1.subList(0, 2));
        expectedToSpillBuffers.put(subpartition2, subpartitionBuffer2.subList(0, 2));
        assertThat(decision.getBufferToSpill()).isEqualTo(expectedToSpillBuffers);

        Map<Integer, List<BufferIndexAndChannel>> expectedToReleaseBuffers = new HashMap<>();
        expectedToReleaseBuffers.put(subpartition1, subpartitionBuffer1.subList(0, 4));
        expectedToReleaseBuffers.put(subpartition2, subpartitionBuffer2.subList(0, 3));
        assertThat(decision.getBufferToRelease()).isEqualTo(expectedToReleaseBuffers);
    }
}
