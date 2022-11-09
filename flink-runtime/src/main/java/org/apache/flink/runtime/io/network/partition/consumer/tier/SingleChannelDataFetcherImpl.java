package org.apache.flink.runtime.io.network.partition.consumer.tier;

import org.apache.flink.runtime.io.network.api.EndOfSegmentEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.tier.common.SingleChannelDataClient;
import org.apache.flink.runtime.io.network.partition.consumer.tier.common.SingleChannelDataClientFactory;
import org.apache.flink.runtime.io.network.partition.consumer.tier.common.SingleChannelDataFetcher;

import java.io.IOException;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkState;

/** The implementation of {@link SingleChannelDataFetcher} interface. */
public class SingleChannelDataFetcherImpl implements SingleChannelDataFetcher {

    private final SingleChannelDataClient localClient;

    private final SingleChannelDataClient dfsClient;

    private long currentSegmentId = 0L;

    public SingleChannelDataFetcherImpl(SingleChannelDataClientFactory clientFactory) {
        this.localClient = clientFactory.createLocalSingleChannelDataClient();
        this.dfsClient = clientFactory.createDfsSingleChannelDataClient();
    }

    @Override
    public Optional<InputGate.InputWithData<InputChannel, InputChannel.BufferAndAvailability>>
            getNextBuffer(InputChannel inputChannel) throws IOException, InterruptedException {
        Optional<BufferAndAvailability> bufferAndAvailability;
        if(localClient.hasSegmentId(inputChannel, currentSegmentId)){
            bufferAndAvailability = localClient.getNextBuffer(
                    inputChannel,
                    currentSegmentId);
        }else if(dfsClient != null && dfsClient.hasSegmentId(inputChannel, currentSegmentId)){
            bufferAndAvailability = dfsClient.getNextBuffer(inputChannel, currentSegmentId);
        }else {
            return Optional.empty();
        }
        if(!bufferAndAvailability.isPresent()){
            return Optional.empty();
        }
        BufferAndAvailability bufferData = bufferAndAvailability.get();
        if (bufferData.buffer().getDataType() == Buffer.DataType.SEGMENT_EVENT) {
            EndOfSegmentEvent endOfSegmentEvent =
                    (EndOfSegmentEvent)
                            EventSerializer.fromSerializedEvent(
                                    bufferData.buffer().getNioBufferReadable(),
                                    Thread.currentThread().getContextClassLoader());
            checkState(endOfSegmentEvent.getSegmentId() == (currentSegmentId + 1L));
            currentSegmentId++;
            bufferData.buffer().recycleBuffer();
            return getNextBuffer(inputChannel);
        }
        return Optional.of(
                new InputGate.InputWithData<>(
                        inputChannel,
                        bufferAndAvailability.get(),
                        true,
                        false));
    }

    @Override
    public void close() throws IOException {
        if(localClient != null){
            localClient.close();
        }
        if(dfsClient != null){
            dfsClient.close();
        }
    }
}
