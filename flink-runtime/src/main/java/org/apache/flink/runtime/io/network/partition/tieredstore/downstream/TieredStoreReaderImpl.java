package org.apache.flink.runtime.io.network.partition.tieredstore.downstream;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.memory.MemorySegmentProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

/** The implementation of {@link TieredStoreReader} interface. */
public class TieredStoreReaderImpl implements TieredStoreReader {

    private final SubpartitionReader[] subpartitionReaders;

    private final int numInputChannels;

    private final SubpartitionTierClientFactory clientFactory;

    public TieredStoreReaderImpl(
            JobID jobID,
            List<ResultPartitionID> resultPartitionIDs,
            MemorySegmentProvider memorySegmentProvider,
            List<Integer> subpartitionIndexes,
            String baseRemoteStoragePath,
            int numInputChannels) {
        this.numInputChannels = numInputChannels;
        this.subpartitionReaders = new SubpartitionReader[numInputChannels];
        this.clientFactory =
                new SubpartitionTierClientFactory(
                        jobID,
                        resultPartitionIDs,
                        memorySegmentProvider,
                        subpartitionIndexes,
                        baseRemoteStoragePath);
    }

    @Override
    public void setup(InputChannel[] channels, Consumer<InputChannel> channelEnqueueReceiver) {
        this.clientFactory.setup(channels, channelEnqueueReceiver);
        for (int i = 0; i < numInputChannels; ++i) {
            subpartitionReaders[i] = new SubpartitionReaderImpl(clientFactory);
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
