package org.apache.flink.runtime.io.network.partition.tieredstore.downstream.common;

import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;

import java.io.IOException;
import java.util.Optional;

/** The interface of {@link SingleChannelTierClient} in Tiered Store. */
public interface SingleChannelTierClient {

    //boolean hasSegmentId(InputChannel inputChannel, int segmentId) throws IOException;

    Optional<InputChannel.BufferAndAvailability> getNextBuffer(
            InputChannel inputChannel, int segmentId) throws IOException, InterruptedException;

    void close() throws IOException;
}
