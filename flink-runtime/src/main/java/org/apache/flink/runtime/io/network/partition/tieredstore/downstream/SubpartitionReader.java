package org.apache.flink.runtime.io.network.partition.tieredstore.downstream;

import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;

import java.io.IOException;
import java.util.Optional;

/** The interface of {@link SubpartitionReader} in Tiered Store. */
public interface SubpartitionReader {

    void setup();

    Optional<BufferAndAvailability> getNextBuffer(InputChannel inputChannel)
            throws IOException, InterruptedException;

    void close() throws IOException;
}