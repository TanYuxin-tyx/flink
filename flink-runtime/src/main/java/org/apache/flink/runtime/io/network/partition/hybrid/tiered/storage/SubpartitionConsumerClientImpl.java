package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

/** The implementation of {@link SubpartitionConsumerClient} interface. */
public class SubpartitionConsumerClientImpl implements SubpartitionConsumerClient {

    private final Consumer<Integer> queueChannelReceiver;

    private final List<TierConsumerAgent> clientList;

    private int currentSegmentId = 0;

    public SubpartitionConsumerClientImpl(
            List<TierConsumerAgent> clientList, Consumer<Integer> queueChannelReceiver) {
        this.clientList = clientList;
        this.queueChannelReceiver = queueChannelReceiver;
    }

    @Override
    public Optional<BufferAndAvailability> getNextBuffer(InputChannel inputChannel)
            throws IOException, InterruptedException {
        Optional<BufferAndAvailability> bufferAndAvailability = Optional.empty();
        for (TierConsumerAgent client : clientList) {
            bufferAndAvailability = client.getNextBuffer(inputChannel, currentSegmentId);
            if (bufferAndAvailability.isPresent()) {
                break;
            }
        }
        if (!bufferAndAvailability.isPresent()) {
            return Optional.empty();
        }
        BufferAndAvailability bufferData = bufferAndAvailability.get();
        if (bufferData.buffer().getDataType() == Buffer.DataType.ADD_SEGMENT_ID_EVENT) {
            currentSegmentId++;
            bufferData.buffer().recycleBuffer();
            queueChannelReceiver.accept(inputChannel.getChannelIndex());
            return getNextBuffer(inputChannel);
        }
        return Optional.of(bufferData);
    }

    @Override
    public void close() throws IOException {
        for (TierConsumerAgent client : clientList) {
            client.close();
        }
    }
}
