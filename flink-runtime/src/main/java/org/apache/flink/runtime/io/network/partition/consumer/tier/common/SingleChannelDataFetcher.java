package org.apache.flink.runtime.io.network.partition.consumer.tier.common;

import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate.InputWithData;

import java.io.IOException;
import java.util.Optional;

/** The interface of {@link SingleChannelDataFetcher} in Tiered Store. */
public interface SingleChannelDataFetcher {
    Optional<InputWithData<InputChannel, BufferAndAvailability>> getNextBuffer(
            InputChannel inputChannel) throws IOException, InterruptedException;

    void close() throws IOException;
}
