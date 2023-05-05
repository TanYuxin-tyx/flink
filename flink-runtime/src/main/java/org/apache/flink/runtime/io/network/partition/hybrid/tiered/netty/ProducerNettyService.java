package org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.BufferContext;

import java.util.Optional;
import java.util.Queue;

/** The implementation of {@link NettyService} in producer side. */
public class ProducerNettyService implements NettyService {

    @Override
    public NettyServiceView register(
            Queue<BufferContext> bufferQueue,
            BufferAvailabilityListener availabilityListener,
            Runnable releaseNotifier) {
        return new NettyServiceViewImpl(bufferQueue, releaseNotifier, availabilityListener);
    }

    @Override
    public Optional<Buffer> readBuffer(int subpartitionId) {
        throw new UnsupportedOperationException("Not supported in producer side.");
    }

    @Override
    public void notifyResultSubpartitionAvailable(int subpartitionId, boolean priority) {
        throw new UnsupportedOperationException("Not supported in producer side.");
    }

    @Override
    public void notifyRequiredSegmentId(int subpartitionId, int segmentId) {
        throw new UnsupportedOperationException("Not supported in producer side.");
    }
}
