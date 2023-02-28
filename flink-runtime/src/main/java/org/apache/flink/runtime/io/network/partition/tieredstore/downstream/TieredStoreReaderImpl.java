package org.apache.flink.runtime.io.network.partition.tieredstore.downstream;

import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.tieredstore.downstream.common.SingleChannelReader;
import org.apache.flink.runtime.io.network.partition.tieredstore.downstream.common.SingleChannelTierClientFactory;
import org.apache.flink.runtime.io.network.partition.tieredstore.downstream.common.TieredStoreReader;

import java.io.IOException;
import java.util.Optional;

/** The implementation of {@link TieredStoreReader} interface. */
public class TieredStoreReaderImpl implements TieredStoreReader {

    private final SingleChannelReader[] singleChannelReaders;

    private final int numInputChannels;

    private final SingleChannelTierClientFactory clientFactory;

    public TieredStoreReaderImpl(
            int numInputChannels, SingleChannelTierClientFactory clientFactory) {
        this.numInputChannels = numInputChannels;
        this.singleChannelReaders = new SingleChannelReader[numInputChannels];
        this.clientFactory = clientFactory;
    }

    @Override
    public void setup() throws IOException {
        for (int i = 0; i < numInputChannels; ++i) {
            singleChannelReaders[i] = new SingleChannelReaderImpl(clientFactory);
            singleChannelReaders[i].setup();
        }
    }

    @Override
    public Optional<InputChannel.BufferAndAvailability> getNextBuffer(InputChannel inputChannel)
            throws IOException, InterruptedException {
        return singleChannelReaders[inputChannel.getChannelIndex()].getNextBuffer(inputChannel);
    }

    @Override
    public void close() throws IOException {
        for (SingleChannelReader singleChannelReader : singleChannelReaders) {
            singleChannelReader.close();
        }
    }
}
