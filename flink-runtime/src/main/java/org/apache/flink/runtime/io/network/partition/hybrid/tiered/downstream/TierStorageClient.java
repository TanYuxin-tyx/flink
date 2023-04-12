package org.apache.flink.runtime.io.network.partition.hybrid.tiered.downstream;

import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;

import java.io.IOException;
import java.util.Optional;

/** The interface of {@link TierStorageClient} in Tiered Store. */
public interface TierStorageClient {

    Optional<InputChannel.BufferAndAvailability> getNextBuffer(
            InputChannel inputChannel, int segmentId) throws IOException, InterruptedException;

    void close() throws IOException;
}
