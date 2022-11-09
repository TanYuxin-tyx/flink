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

import org.apache.flink.runtime.io.network.partition.store.TieredStoreConfiguration;
import org.apache.flink.runtime.io.network.partition.store.common.BufferIndexAndChannel;
import org.apache.flink.runtime.io.network.partition.store.tier.local.file.BufferSpillingInfoProvider.ConsumeStatusWithId;
import org.apache.flink.runtime.io.network.partition.store.tier.local.file.BufferSpillingInfoProvider.SpillStatus;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;
import java.util.TreeMap;

/** A special implementation of {@link TsSpillingStrategy} that spilled all buffers to disk. */
public class FullSpillingStrategy implements TsSpillingStrategy {
    private final float numBuffersTriggerSpillingRatio;

    private final float releaseBufferRatio;

    private final float releaseThreshold;

    public FullSpillingStrategy(TieredStoreConfiguration storeConfiguration) {
        this.numBuffersTriggerSpillingRatio = storeConfiguration.getFullStrategyNumBuffersTriggerSpillingRatio();
        this.releaseThreshold = storeConfiguration.getFullStrategyReleaseThreshold();
        this.releaseBufferRatio = storeConfiguration.getFullStrategyReleaseBufferRatio();
    }

    // For the case of buffer finished, whenever the number of unSpillBuffers reaches
    // numBuffersTriggerSpillingRatio times currentPoolSize, make a decision based on global
    // information. Otherwise, no need to take action.
    @Override
    public Optional<Decision> onBufferFinished(int numTotalUnSpillBuffers, int currentPoolSize) {
        return Optional.empty();
    }

    // For the case of buffer consumed, there is no need to take action for HsFullSpillingStrategy.
    @Override
    public Optional<Decision> onBufferConsumed(BufferIndexAndChannel consumedBuffer) {
        return Optional.of(Decision.NO_ACTION);
    }

    @Override
    public Decision forceTriggerFlushCachedBuffers(
            BufferSpillingInfoProvider spillingInfoProvider) {
        return onResultPartitionClosed(spillingInfoProvider);
    }

    @Override
    public Decision onResultPartitionClosed(BufferSpillingInfoProvider spillingInfoProvider) {
        Decision.Builder builder = Decision.builder();
        for (int subpartitionId = 0;
             subpartitionId < spillingInfoProvider.getNumSubpartitions(); subpartitionId++) {
            Deque<BufferIndexAndChannel> buffersInOrder = spillingInfoProvider.getBuffersInOrder(subpartitionId,
                    SpillStatus.NOT_SPILL,
                    ConsumeStatusWithId.ALL_ANY);
            builder
                    .addBufferToSpill(subpartitionId, buffersInOrder)
                    .addBufferToRelease(subpartitionId, buffersInOrder);
        }
        return builder.build();
    }

    private void checkSpill(
            BufferSpillingInfoProvider spillingInfoProvider,
            int poolSize,
            Decision.Builder builder) {
        if (spillingInfoProvider.getNumTotalUnSpillBuffers()
                < numBuffersTriggerSpillingRatio * poolSize) {
            // In case situation changed since onBufferFinished() returns Optional#empty()
            return;
        }
        // Spill all not spill buffers.
        getToSpillBuffers(spillingInfoProvider, builder);
    }

    void getToSpillBuffers(
            BufferSpillingInfoProvider spillingInfoProvider, Decision.Builder builder) {
        for (int i = 0; i < spillingInfoProvider.getNumSubpartitions(); i++) {
            builder.addBufferToSpill(i,
                    spillingInfoProvider.getBuffersInOrder(i,
                            SpillStatus.NOT_SPILL,
                            ConsumeStatusWithId.ALL_ANY));
        }
    }

    private void getToReleaseBuffers(
            BufferSpillingInfoProvider spillingInfoProvider,
            int poolSize,
            Decision.Builder builder) {
        int survivedNum = (int) (poolSize - poolSize * releaseBufferRatio);
        int numSubpartitions = spillingInfoProvider.getNumSubpartitions();
        int subpartitionSurvivedNum = survivedNum / numSubpartitions;

        TreeMap<Integer, Deque<BufferIndexAndChannel>> bufferToRelease = new TreeMap<>();

        for (int subpartitionId = 0; subpartitionId < numSubpartitions; subpartitionId++) {
            Deque<BufferIndexAndChannel> buffersInOrder = spillingInfoProvider.getBuffersInOrder(
                    subpartitionId,
                    SpillStatus.SPILL,
                    ConsumeStatusWithId.ALL_ANY);
            // if the number of subpartition buffers less than survived buffers, reserved all of
            // them.
            int releaseNum = Math.max(0, buffersInOrder.size() - subpartitionSurvivedNum);
            while (releaseNum-- != 0) {
                buffersInOrder.pollLast();
            }
            bufferToRelease.put(subpartitionId, buffersInOrder);
        }

        // collect results in order
        for (int i = 0; i < numSubpartitions; i++) {
            builder.addBufferToRelease(i, bufferToRelease.getOrDefault(i, new ArrayDeque<>()));
        }
    }
}
