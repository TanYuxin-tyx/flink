package org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty;

import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.BufferContext;

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
    public BufferAndAvailability readBuffer(int subpartitionId) {
        throw new UnsupportedOperationException(
                "Netty service in producer side doesn't support read buffer");
    }
}
