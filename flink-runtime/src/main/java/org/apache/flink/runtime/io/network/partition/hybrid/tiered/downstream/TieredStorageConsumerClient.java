package org.apache.flink.runtime.io.network.partition.hybrid.tiered.downstream;

import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;

import java.io.IOException;
import java.util.Optional;
import java.util.function.Consumer;

/** The interface of {@link TieredStorageConsumerClient} in Tiered Store. */
public interface TieredStorageConsumerClient {

    void setup(InputChannel[] inputChannels, Consumer<InputChannel> channelEnqueuer);

    void start();

    Optional<BufferAndAvailability> getNextBuffer(InputChannel inputChannel)
            throws IOException, InterruptedException;

    void close() throws IOException;
}
