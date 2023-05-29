package org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty2;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;
import java.util.Optional;
import java.util.function.BiConsumer;

public class ConsumerNettyServiceImpl implements ConsumerNettyService {

    private InputChannel[] inputChannels;

    private BiConsumer<Integer, Boolean> subpartitionAvailableNotifier;

    private int[] lastPrioritySequenceNumber;

    @Override
    public void setup(
            InputChannel[] inputChannels,
            int[] lastPrioritySequenceNumber,
            BiConsumer<Integer, Boolean> subpartitionAvailableNotifier) {
        this.inputChannels = inputChannels;
        this.lastPrioritySequenceNumber = lastPrioritySequenceNumber;
        this.subpartitionAvailableNotifier = subpartitionAvailableNotifier;
    }

    @Override
    public Optional<Buffer> readBuffer(int subpartitionId) {
        Optional<InputChannel.BufferAndAvailability> bufferAndAvailability = Optional.empty();
        try {
            bufferAndAvailability = inputChannels[subpartitionId].getNextBuffer();
        } catch (IOException | InterruptedException e) {
            ExceptionUtils.rethrow(e, "Failed to read buffer in Consumer Netty Service.");
        }
        if (bufferAndAvailability.isPresent()) {
            if (bufferAndAvailability.get().moreAvailable()) {
                notifyResultSubpartitionAvailable(
                        subpartitionId, bufferAndAvailability.get().hasPriority());
            }
            if (bufferAndAvailability.get().hasPriority()) {
                lastPrioritySequenceNumber[subpartitionId] =
                        bufferAndAvailability.get().getSequenceNumber();
            }
        }
        return bufferAndAvailability.map(InputChannel.BufferAndAvailability::buffer);
    }

    @Override
    public void notifyResultSubpartitionAvailable(int subpartitionId, boolean priority) {
        subpartitionAvailableNotifier.accept(subpartitionId, priority);
    }

    @Override
    public void notifyRequiredSegmentId(int subpartitionId, int segmentId) {
        inputChannels[subpartitionId].notifyRequiredSegmentId(segmentId);
    }
}
