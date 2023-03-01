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

package org.apache.flink.runtime.io.network.partition.tieredstore.downstream;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.memory.MemorySegmentProvider;
import org.apache.flink.runtime.executiongraph.IndexRange;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferDecompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.tieredstore.downstream.common.TieredStoreReader;
import org.apache.flink.runtime.io.network.partition.tieredstore.downstream.common.SingleChannelTierClientFactory;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.throughput.BufferDebloater;
import org.apache.flink.runtime.throughput.ThroughputCalculator;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/** The input gate for Tiered Store. */
public class TieredStoreSingleInputGate extends SingleInputGate {

    private final TieredStoreReader tieredStoreReader;

    private final SingleChannelTierClientFactory clientFactory;

    public TieredStoreSingleInputGate(
            String owningTaskName,
            int gateIndex,
            int subpartitionIndex,
            IntermediateDataSetID consumedResultId,
            ResultPartitionType consumedPartitionType,
            IndexRange subpartitionIndexRange,
            int numberOfInputChannels,
            PartitionProducerStateProvider partitionProducerStateProvider,
            SupplierWithException<BufferPool, IOException> bufferPoolFactory,
            @Nullable BufferDecompressor bufferDecompressor,
            MemorySegmentProvider memorySegmentProvider,
            int segmentSize,
            ThroughputCalculator throughputCalculator,
            @Nullable BufferDebloater bufferDebloater,
            JobID jobID,
            List<ResultPartitionID> resultPartitionIDs,
            String baseDfsPath) {
        super(
                owningTaskName,
                gateIndex,
                consumedResultId,
                consumedPartitionType,
                subpartitionIndexRange,
                numberOfInputChannels,
                partitionProducerStateProvider,
                bufferPoolFactory,
                bufferDecompressor,
                memorySegmentProvider,
                segmentSize,
                throughputCalculator,
                bufferDebloater);

        this.clientFactory =
                new SingleChannelTierClientFactory(
                        jobID,
                        resultPartitionIDs,
                        getMemorySegmentProvider(),
                        subpartitionIndex,
                        baseDfsPath);

        this.tieredStoreReader = new TieredStoreReaderImpl(numberOfInputChannels, clientFactory);
    }

    @Override
    public void setup() throws IOException {
        super.setup();
        tieredStoreReader.setup();
    }

    @Override
    public Optional<InputWithData<InputChannel, InputChannel.BufferAndAvailability>>
            waitAndGetNextData(boolean blocking) throws IOException, InterruptedException {
        while (true) {
            synchronized (inputChannelsWithData) {
                Optional<InputChannel.BufferAndAvailability> bufferAndAvailabilityOpt;
                InputChannel inputChannel;
                Optional<InputChannel> inputChannelOpt = getChannel(blocking);
                if (!inputChannelOpt.isPresent()) {
                    return Optional.empty();
                }
                inputChannel = inputChannelOpt.get();
                enqueueChannelWhenSatisfyCondition(inputChannel);
                bufferAndAvailabilityOpt = tieredStoreReader.getNextBuffer(inputChannel);
                if (!bufferAndAvailabilityOpt.isPresent()) {
                    checkUnavailability();
                    continue;
                }
                final InputChannel.BufferAndAvailability bufferAndAvailability =
                        bufferAndAvailabilityOpt.get();
                if (bufferAndAvailability.moreAvailable()) {
                    queueChannelUnsafe(inputChannel, bufferAndAvailability.morePriorityEvents());
                }
                final boolean morePriorityEvents =
                        inputChannelsWithData.getNumPriorityElements() > 0;
                if (bufferAndAvailability.hasPriority()) {
                    lastPrioritySequenceNumber[inputChannel.getChannelIndex()] =
                            bufferAndAvailability.getSequenceNumber();
                    if (!morePriorityEvents) {
                        priorityAvailabilityHelper.resetUnavailable();
                    }
                }
                if (bufferAndAvailability.buffer().getDataType() == Buffer.DataType.SEGMENT_EVENT) {
                    bufferAndAvailability.buffer().recycleBuffer();
                    queueChannelUnsafe(inputChannel, false);
                    continue;
                }
                checkUnavailability();
                return Optional.of(
                        new InputGate.InputWithData<>(
                                inputChannel,
                                bufferAndAvailability,
                                !inputChannelsWithData.isEmpty(),
                                morePriorityEvents));
            }
        }
    }

    @Override
    public void requestPartitions() {
        super.requestPartitions();
        for (InputChannel inputChannel : inputChannels.values()) {
            if (inputChannel.getClass() == RemoteInputChannel.class) {
                synchronized (inputChannelsWithData) {
                    queueChannelUnsafe(inputChannel, false);
                }
                markAvailable();
            }
        }
    }

    @Override
    public void close() throws IOException {
        super.close();
        tieredStoreReader.close();
    }

    // ------------------------------------
    //           Internal Method
    // ------------------------------------

    private void enqueueChannelWhenSatisfyCondition(InputChannel inputChannel) {
        // The input channel will be enqueued when satisfied all the following conditions:
        // 1. The current thread has not been interrupted because of fail over.
        // 2. The Remote Tier is enabled.
        // 3. The InputChannel is LocalInputChannel or RemoteInputChannel.
        if (!Thread.currentThread().isInterrupted()
                && clientFactory.isEnableRemoteTier()
                && (inputChannel.getClass() == LocalInputChannel.class
                        || inputChannel.getClass() == RemoteInputChannel.class)) {
            queueChannelUnsafe(inputChannel, false);
        }
    }
}
