package org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.BufferContext;

import java.util.Optional;
import java.util.Queue;

/**
 * The {@link NettyService} is used to provide the netty-based network services in the shuffle
 * process of tiered-store.
 */
public interface NettyService {

    // ------------------------------------
    //        For Producer Side
    // ------------------------------------

    NettyServiceView register(
            Queue<BufferContext> bufferQueue,
            BufferAvailabilityListener availabilityListener,
            Runnable serviceReleaseNotifier);

    // ------------------------------------
    //        For Consumer Side
    // ------------------------------------

    Optional<Buffer> readBuffer(int subpartitionId);

    void notifyResultSubpartitionAvailable(int subpartitionId, boolean priority);

    void notifyRequiredSegmentId(int subpartitionId, int segmentId);
}
