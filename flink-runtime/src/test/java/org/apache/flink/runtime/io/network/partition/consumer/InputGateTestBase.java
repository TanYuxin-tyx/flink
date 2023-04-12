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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.io.PullingAsyncDataInput;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.buffer.BufferDecompressor;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.TieredStoreShuffleEnvironment;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.downstream.TierReaderFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.downstream.TieredStoreReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.downstream.TieredStoreReaderImpl;

import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Test base for {@link InputGate}. */
public abstract class InputGateTestBase {

    int gateIndex;

    @Before
    public void resetGateIndex() {
        gateIndex = 0;
    }

    protected void testIsAvailable(
            InputGate inputGateToTest,
            SingleInputGate inputGateToNotify,
            TestInputChannel inputChannelWithNewData)
            throws Exception {

        assertFalse(inputGateToTest.getAvailableFuture().isDone());
        assertFalse(inputGateToTest.pollNext().isPresent());

        CompletableFuture<?> future = inputGateToTest.getAvailableFuture();

        assertFalse(inputGateToTest.getAvailableFuture().isDone());
        assertFalse(inputGateToTest.pollNext().isPresent());

        assertEquals(future, inputGateToTest.getAvailableFuture());

        inputChannelWithNewData.readBuffer();
        inputGateToNotify.notifyChannelNonEmpty(inputChannelWithNewData);

        assertTrue(future.isDone());
        assertTrue(inputGateToTest.getAvailableFuture().isDone());
        assertEquals(PullingAsyncDataInput.AVAILABLE, inputGateToTest.getAvailableFuture());
    }

    protected void testIsAvailableAfterFinished(
            InputGate inputGateToTest, Runnable endOfPartitionEvent) throws Exception {

        CompletableFuture<?> available = inputGateToTest.getAvailableFuture();
        assertFalse(available.isDone());
        assertFalse(inputGateToTest.pollNext().isPresent());

        endOfPartitionEvent.run();

        assertTrue(inputGateToTest.pollNext().isPresent()); // EndOfPartitionEvent

        assertTrue(available.isDone());
        assertTrue(inputGateToTest.getAvailableFuture().isDone());
        assertEquals(PullingAsyncDataInput.AVAILABLE, inputGateToTest.getAvailableFuture());
    }

    protected SingleInputGate createInputGate() {
        return createInputGate(2);
    }

    protected SingleInputGate createInputGate(int numberOfInputChannels) {
        return createInputGate(null, numberOfInputChannels, ResultPartitionType.PIPELINED);
    }

    protected SingleInputGate createInputGate(
            NettyShuffleEnvironment environment,
            int numberOfInputChannels,
            ResultPartitionType partitionType) {

        SingleInputGateBuilder builder =
                new SingleInputGateBuilder()
                        .setNumberOfChannels(numberOfInputChannels)
                        .setSingleInputGateIndex(gateIndex++)
                        .setResultPartitionType(partitionType);

        if (environment != null) {
            builder = builder.setupBufferPoolFactory(environment);
        }

        SingleInputGate inputGate = builder.build();
        assertEquals(partitionType, inputGate.getConsumedPartitionType());
        return inputGate;
    }

    protected SingleInputGate createInputGateWithTieredStoreReader(
            NettyShuffleEnvironment environment,
            int numberOfInputChannels,
            ResultPartitionType partitionType,
            String baseRemoteStoragePath) {
        return createInputGateWithTieredStoreReader(
                environment, numberOfInputChannels, partitionType, baseRemoteStoragePath, null);
    }

    protected SingleInputGate createInputGateWithTieredStoreReader(
            NettyShuffleEnvironment environment,
            int numberOfInputChannels,
            ResultPartitionType partitionType,
            String baseRemoteStoragePath,
            BufferDecompressor bufferDecompressor) {

        List<Integer> subpartitionIndexes = new ArrayList<>();
        for (int index = 0; index < numberOfInputChannels; ++index) {
            subpartitionIndexes.add(index);
        }

        TieredStoreShuffleEnvironment storeShuffleEnvironment = new TieredStoreShuffleEnvironment();
        TierReaderFactory tierReaderFactory =
                storeShuffleEnvironment.createStorageTierReaderFactory(
                        JobID.generate(),
                        Collections.singletonList(new ResultPartitionID()),
                        environment.getNetworkBufferPool(),
                        subpartitionIndexes,
                        baseRemoteStoragePath);

        TieredStoreReader tieredStoreReader =
                new TieredStoreReaderImpl(numberOfInputChannels, tierReaderFactory);

        SingleInputGateBuilder builder =
                new SingleInputGateBuilder()
                        .setNumberOfChannels(numberOfInputChannels)
                        .setSingleInputGateIndex(gateIndex++)
                        .setResultPartitionType(partitionType)
                        .setTieredStoreReader(tieredStoreReader)
                        .setupBufferPoolFactory(environment)
                        .setBufferDecompressor(bufferDecompressor);

        SingleInputGate inputGate = builder.build();
        assertEquals(partitionType, inputGate.getConsumedPartitionType());
        return inputGate;
    }
}
