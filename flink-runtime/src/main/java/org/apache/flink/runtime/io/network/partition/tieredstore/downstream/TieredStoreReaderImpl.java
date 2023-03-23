package org.apache.flink.runtime.io.network.partition.tieredstore.downstream;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.memory.MemorySegmentProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.tieredstore.downstream.common.SingleChannelReader;
import org.apache.flink.runtime.io.network.partition.tieredstore.downstream.common.SingleChannelTierClientFactory;
import org.apache.flink.runtime.io.network.partition.tieredstore.downstream.common.TieredStoreReader;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

/** The implementation of {@link TieredStoreReader} interface. */
public class TieredStoreReaderImpl implements TieredStoreReader {

    private final SingleChannelReader[] singleChannelReaders;

    private final int numInputChannels;

    private final SingleChannelTierClientFactory clientFactory;

    private final Consumer<InputChannel> channelEnqueueReceiver;

    public TieredStoreReaderImpl(
            JobID jobID,
            List<ResultPartitionID> resultPartitionIDs,
            MemorySegmentProvider memorySegmentProvider,
            List<Integer> subpartitionIndexes,
            String baseRemoteStoragePath,
            int numInputChannels,
            Consumer<InputChannel> channelEnqueueReceiver) {
        this.numInputChannels = numInputChannels;
        this.singleChannelReaders = new SingleChannelReader[numInputChannels];
        this.clientFactory = new SingleChannelTierClientFactory(
                        jobID,
                        resultPartitionIDs,
                        memorySegmentProvider,
                        subpartitionIndexes,
                        baseRemoteStoragePath);
        this.channelEnqueueReceiver = channelEnqueueReceiver;
    }

    @Override
    public void setup(InputChannel[] channels) {
        this.clientFactory.setup(channels, channelEnqueueReceiver);
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
