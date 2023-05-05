package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier;

import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;

import java.io.IOException;
import java.util.Optional;

/** The interface of {@link TierConsumerAgent} in Tiered Store. */
public interface TierConsumerAgent {

    void start();

    Optional<InputChannel.BufferAndAvailability> getNextBuffer(
            int subpartitionId, int segmentId) throws IOException, InterruptedException;

    void close() throws IOException;
}
