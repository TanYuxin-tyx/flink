package org.apache.flink.runtime.io.network.partition.tieredstore.downstream;

import org.apache.flink.runtime.io.network.api.EndOfSegmentEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkState;

/** The implementation of {@link SubpartitionReader} interface. */
public class SubpartitionReaderImpl implements SubpartitionReader {

    private final SubpartitionTierClientFactory clientFactory;

    private List<SubpartitionTierClient> clientList;

    private int currentSegmentId = 0;

    public SubpartitionReaderImpl(SubpartitionTierClientFactory clientFactory) {
        this.clientFactory = clientFactory;
    }

    @Override
    public void setup() {
        this.clientList = clientFactory.createClientList();
    }

    @Override
    public Optional<InputChannel.BufferAndAvailability> getNextBuffer(InputChannel inputChannel)
            throws IOException, InterruptedException {
        Optional<BufferAndAvailability> bufferAndAvailability = Optional.empty();
        for (SubpartitionTierClient client : clientList) {
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
            checkState(
                    getSegmentId(bufferData) == (currentSegmentId + 1),
                    "Received illegal segmentId");
            currentSegmentId++;
        }
        return Optional.of(bufferData);
    }

    @Override
    public void close() throws IOException {
        for (SubpartitionTierClient client : clientList) {
            client.close();
        }
    }

    // ------------------------------------
    //           Internal Method
    // ------------------------------------

    private int getSegmentId(BufferAndAvailability bufferData) throws IOException {
        return ((EndOfSegmentEvent)
                        EventSerializer.fromSerializedEvent(
                                bufferData.buffer().getNioBufferReadable(),
                                Thread.currentThread().getContextClassLoader()))
                .getSegmentId();
    }
}
