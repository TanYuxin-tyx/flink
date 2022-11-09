package org.apache.flink.runtime.io.network.partition.consumer.tier;

import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.tier.common.DataFetcher;
import org.apache.flink.runtime.io.network.partition.consumer.tier.common.SingleChannelDataClientFactory;
import org.apache.flink.runtime.io.network.partition.consumer.tier.common.SingleChannelDataFetcher;

import java.io.IOException;
import java.util.Optional;

/** The implementation of {@link DataFetcher} interface. */
public class DataFetcherImpl implements DataFetcher {

    private final SingleChannelDataFetcher[] singleChannelDataFetchers;

    private final int numInputChannels;

    private final SingleChannelDataClientFactory clientFactory;

    public DataFetcherImpl(int numInputChannels, SingleChannelDataClientFactory clientFactory) {
        this.numInputChannels = numInputChannels;
        this.singleChannelDataFetchers = new SingleChannelDataFetcher[numInputChannels];
        this.clientFactory = clientFactory;
    }

    @Override
    public void setup() throws IOException {
        for (int i = 0; i < numInputChannels; ++i) {
            singleChannelDataFetchers[i] = new SingleChannelDataFetcherImpl(clientFactory);
        }
    }

    @Override
    public Optional<InputGate.InputWithData<InputChannel, InputChannel.BufferAndAvailability>>
            getNextBuffer(InputChannel inputChannel) throws IOException, InterruptedException {
        return singleChannelDataFetchers[inputChannel.getChannelIndex()].getNextBuffer(
                inputChannel);
    }

    @Override
    public void close() throws IOException {
        for(SingleChannelDataFetcher singleChannelDataFetcher : singleChannelDataFetchers){
            singleChannelDataFetcher.close();
        }
    }
}
