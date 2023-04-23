package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.downstream;

import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;

import java.io.IOException;
import java.util.Optional;

/** The interface of {@link SubConsumerClient} in Tiered Store. */
public interface SubConsumerClient {

    Optional<BufferAndAvailability> getNextBuffer(InputChannel inputChannel)
            throws IOException, InterruptedException;

    void close() throws IOException;
}
