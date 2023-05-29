package org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty2;

import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.CreditBasedShuffleView;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.BufferContext;

import java.util.Queue;

/** {@link ProducerNettyService} is used to transfer buffer to netty server in producer side. */
public interface ProducerNettyService {

    /**
     * Register a buffer queue to the NettyService and the buffer will be consumed from the queue
     * and sent to netty.
     *
     * @param bufferQueue is the required queue.
     * @param serviceReleaseNotifier is used to notify that the service is released.
     */
    void register(
            int subpartitionId, Queue<BufferContext> bufferQueue, Runnable serviceReleaseNotifier);

    /**
     * Create a view of buffers in the specific subpartition, which will be used to transfer buffer
     * in netty server with the credit-based protocol.
     *
     * @param subpartitionId subpartition id indicates the id of subpartition.
     * @param availabilityListener availabilityListener is used to listen the available status of
     *     buffer queue.
     * @return the with the credit-based protocol.
     */
    CreditBasedShuffleView createCreditBasedShuffleView(
            int subpartitionId, BufferAvailabilityListener availabilityListener);
}
