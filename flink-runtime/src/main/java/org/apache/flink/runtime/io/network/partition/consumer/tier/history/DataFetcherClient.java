package org.apache.flink.runtime.io.network.partition.consumer.tier.history;

import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;

import java.io.IOException;
import java.util.Optional;

/**
 * The data fetcher client interface for Tiered Store, which can fetch shuffle data on Memory Tier,
 * or Local Tier, or Dfs Tier.
 */
public interface DataFetcherClient {

    void setup() throws IOException;

    Optional<InputGate.InputWithData<InputChannel, InputChannel.BufferAndAvailability>>
            waitAndGetNextData(boolean blocking) throws IOException, InterruptedException;

    void close() throws IOException;
}
