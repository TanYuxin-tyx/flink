package org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty;

import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.BufferContext;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;
import java.util.Optional;
import java.util.Queue;
import java.util.function.Consumer;

/** The implementation of {@link NettyService}. */
public class ConsumerNettyService implements NettyService {

    private final InputChannel[] inputChannels;

    private final Consumer<Integer> subpartitionAvailableNotifier;

    public ConsumerNettyService(
            InputChannel[] inputChannels, Consumer<Integer> subpartitionAvailableNotifier) {
        this.inputChannels = inputChannels;
        this.subpartitionAvailableNotifier = subpartitionAvailableNotifier;
    }

    @Override
    public Optional<BufferAndAvailability> readBuffer(int subpartitionId) {
        Optional<BufferAndAvailability> bufferAndAvailability = Optional.empty();
        try {
            bufferAndAvailability = inputChannels[subpartitionId].getNextBuffer();
        } catch (IOException | InterruptedException e) {
            ExceptionUtils.rethrow(e, "Failed to read buffer in Consumer Netty Service.");
        }
        return bufferAndAvailability;
    }

    @Override
    public void notifyResultSubpartitionAvailable(int subpartitionId) {
        subpartitionAvailableNotifier.accept(subpartitionId);
    }

    @Override
    public NettyServiceView register(
            Queue<BufferContext> bufferQueue,
            BufferAvailabilityListener availabilityListener,
            Runnable releaseNotifier) {
        throw new UnsupportedOperationException("Not supported in consumer side.");
    }
}
