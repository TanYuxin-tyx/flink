package org.apache.flink.runtime.io.network.partition.tieredstore.downstream;

import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;

import java.io.IOException;
import java.util.Optional;
import java.util.function.Consumer;

/** The implementation of {@link TieredStoreReader} interface. */
public class TieredStoreReaderImpl implements TieredStoreReader {

    private final SubpartitionReader[] subpartitionReaders;

    private final int numInputChannels;

    private final StorageTierReaderFactory clientFactory;

    public TieredStoreReaderImpl(
            int numInputChannels, StorageTierReaderFactory storageTierReaderFactory) {
        this.numInputChannels = numInputChannels;
        this.subpartitionReaders = new SubpartitionReader[numInputChannels];
        this.clientFactory = storageTierReaderFactory;
    }

    @Override
    public void setup(InputChannel[] channels, Consumer<InputChannel> channelEnqueuer) {
        this.clientFactory.setup(channels, channelEnqueuer);
        for (int i = 0; i < numInputChannels; ++i) {
            subpartitionReaders[i] = new SubpartitionReaderImpl(clientFactory, channelEnqueuer);
            subpartitionReaders[i].setup();
        }
    }

    @Override
    public Optional<InputChannel.BufferAndAvailability> getNextBuffer(InputChannel inputChannel)
            throws IOException, InterruptedException {
        return subpartitionReaders[inputChannel.getChannelIndex()].getNextBuffer(inputChannel);
    }

    @Override
    public void close() throws IOException {
        for (SubpartitionReader subpartitionReader : subpartitionReaders) {
            subpartitionReader.close();
        }
    }
}
