package org.apache.flink.runtime.io.network.partition.hybrid.tiered.downstream;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

/** The implementation of {@link SubpartitionReader} interface. */
public class SubpartitionReaderImpl implements SubpartitionReader {

    private final TierReaderFactory clientFactory;

    private final Consumer<InputChannel> queueChannelReceiver;

    private List<TierReader> clientList;

    private int currentSegmentId = 0;

    public SubpartitionReaderImpl(
            TierReaderFactory clientFactory, Consumer<InputChannel> queueChannelReceiver) {
        this.clientFactory = clientFactory;
        this.queueChannelReceiver = queueChannelReceiver;
    }

    @Override
    public void setup() {
        this.clientList = clientFactory.createClientList();
    }

    @Override
    public Optional<BufferAndAvailability> getNextBuffer(InputChannel inputChannel)
            throws IOException, InterruptedException {
        Optional<BufferAndAvailability> bufferAndAvailability = Optional.empty();
        for (TierReader client : clientList) {
            bufferAndAvailability = client.getNextBuffer(inputChannel, currentSegmentId);
            if (bufferAndAvailability.isPresent()) {
                break;
            }
        }
        if (!bufferAndAvailability.isPresent()) {
            return Optional.empty();
        }
        BufferAndAvailability bufferData = bufferAndAvailability.get();
        if (bufferData.buffer().getDataType() == Buffer.DataType.SEGMENT_EVENT) {
            currentSegmentId++;
            bufferData.buffer().recycleBuffer();
            queueChannelReceiver.accept(inputChannel);
            return getNextBuffer(inputChannel);
        }
        return Optional.of(bufferData);
    }

    @Override
    public void close() throws IOException {
        for (TierReader client : clientList) {
            client.close();
        }
    }
}
