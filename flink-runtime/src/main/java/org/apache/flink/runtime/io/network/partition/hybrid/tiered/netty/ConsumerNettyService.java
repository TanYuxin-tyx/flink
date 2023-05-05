package org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.BufferContext;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;
import java.util.Optional;
import java.util.Queue;
import java.util.function.BiConsumer;

/** The implementation of {@link NettyService}. */
public class ConsumerNettyService implements NettyService {

    private final InputChannel[] inputChannels;

    private final BiConsumer<Integer, Boolean> subpartitionAvailableNotifier;

    public ConsumerNettyService(
            InputChannel[] inputChannels,
            BiConsumer<Integer, Boolean> subpartitionAvailableNotifier) {
        this.inputChannels = inputChannels;
        this.subpartitionAvailableNotifier = subpartitionAvailableNotifier;
    }

    @Override
    public Optional<Buffer> readBuffer(int subpartitionId) {
        Optional<BufferAndAvailability> bufferAndAvailability = Optional.empty();
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
        }
        return bufferAndAvailability.map(BufferAndAvailability::buffer);
    }

    @Override
    public void notifyResultSubpartitionAvailable(int subpartitionId, boolean priority) {
        subpartitionAvailableNotifier.accept(subpartitionId, priority);
    }

    @Override
    public void notifyRequiredSegmentId(int subpartitionId, int segmentId) {
        inputChannels[subpartitionId].notifyRequiredSegmentId(segmentId);
    }

    @Override
    public NettyServiceView register(
            Queue<BufferContext> bufferQueue,
            BufferAvailabilityListener availabilityListener,
            Runnable releaseNotifier) {
        throw new UnsupportedOperationException("Not supported in consumer side.");
    }
}
