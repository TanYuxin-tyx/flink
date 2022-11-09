package org.apache.flink.runtime.io.network.partition.consumer.tier.history;

import org.apache.flink.runtime.io.network.partition.PrioritizedDeque;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;

import java.io.IOException;
import java.util.Optional;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.SEGMENT_EVENT;

/** The data fetcher client for Local Tier. */
public class LocalDataFetcherClient implements DataFetcherClient {

    private final PrioritizedDeque<InputChannel> inputChannelsWithData;

    private final SingleInputGate singleInputGate;

    public LocalDataFetcherClient(
            PrioritizedDeque<InputChannel> inputChannelsWithData, SingleInputGate singleInputGate) {
        this.inputChannelsWithData = inputChannelsWithData;
        this.singleInputGate = singleInputGate;
    }

    @Override
    public void setup() {
        // nothing to do
    }

    public Optional<InputGate.InputWithData<InputChannel, InputChannel.BufferAndAvailability>>
            waitAndGetNextData(boolean blocking) throws IOException, InterruptedException {
        while (true) {
            synchronized (inputChannelsWithData) {
                Optional<InputChannel> inputChannelOpt = singleInputGate.getChannel(blocking);
                if (!inputChannelOpt.isPresent()) {
                    return Optional.empty();
                }

                final InputChannel inputChannel = inputChannelOpt.get();
                Optional<InputChannel.BufferAndAvailability> bufferAndAvailabilityOpt =
                        inputChannel.getNextBuffer();

                if (!bufferAndAvailabilityOpt.isPresent()) {
                    singleInputGate.checkUnavailability();
                    continue;
                }

                final InputChannel.BufferAndAvailability bufferAndAvailability =
                        bufferAndAvailabilityOpt.get();
                // Ignore the data type SEGMENT_EVENT
                if (bufferAndAvailability.buffer().getDataType() == SEGMENT_EVENT) {
                    singleInputGate.queueChannelUnsafe(
                            inputChannel, bufferAndAvailability.morePriorityEvents());
                    continue;
                }
                if (bufferAndAvailability.moreAvailable()) {
                    // enqueue the inputChannel at the end to avoid starvation
                    singleInputGate.queueChannelUnsafe(
                            inputChannel, bufferAndAvailability.morePriorityEvents());
                }

                singleInputGate.checkUnavailability();

                return Optional.of(
                        new InputGate.InputWithData<>(
                                inputChannel,
                                bufferAndAvailability,
                                !inputChannelsWithData.isEmpty(),
                                false));
            }
        }
    }

    @Override
    public void close() {
        // nothing to do
    }
}
