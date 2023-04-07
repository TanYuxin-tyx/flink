package org.apache.flink.runtime.io.network.partition.hybrid.tiered.downstream;

import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;

import java.io.IOException;
import java.util.Optional;

/** The interface of {@link TierReader} in Tiered Store. */
public interface TierReader {

    Optional<InputChannel.BufferAndAvailability> getNextBuffer(
            InputChannel inputChannel, int segmentId) throws IOException, InterruptedException;

    void close() throws IOException;
}
