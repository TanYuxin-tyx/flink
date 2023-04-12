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

    private final Consumer<Integer> queueChannelReceiver;

    private final List<TierStorageClient> clientList;

    private int currentSegmentId = 0;

    public SubpartitionReaderImpl(
            List<TierStorageClient> clientList, Consumer<Integer> queueChannelReceiver) {
        this.clientList = clientList;
        this.queueChannelReceiver = queueChannelReceiver;
    }

    @Override
    public Optional<BufferAndAvailability> getNextBuffer(InputChannel inputChannel)
            throws IOException, InterruptedException {
        Optional<BufferAndAvailability> bufferAndAvailability = Optional.empty();
        for (TierStorageClient client : clientList) {
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
            queueChannelReceiver.accept(inputChannel.getChannelIndex());
            return getNextBuffer(inputChannel);
        }
        return Optional.of(bufferData);
    }

    @Override
    public void close() throws IOException {
        for (TierStorageClient client : clientList) {
            client.close();
        }
    }
}
