package org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty;

import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.BufferContext;

import java.util.Optional;
import java.util.Queue;

/** The implementation of {@link NettyService}. */
public class ProducerNettyService implements NettyService {

    @Override
    public NettyServiceView register(
            Queue<BufferContext> bufferQueue,
            BufferAvailabilityListener availabilityListener,
            Runnable releaseNotifier) {
        return new NettyServiceViewImpl(bufferQueue, releaseNotifier, availabilityListener);
    }

    @Override
    public Optional<InputChannel.BufferAndAvailability> readBuffer(int subpartitionId) {
        throw new UnsupportedOperationException("Not supported in producer side.");
    }

    @Override
    public void notifyResultSubpartitionAvailable(int subpartitionId) {
        throw new UnsupportedOperationException("Not supported in producer side.");
    }
}
