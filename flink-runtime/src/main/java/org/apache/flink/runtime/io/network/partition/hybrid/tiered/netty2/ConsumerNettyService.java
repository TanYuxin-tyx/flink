package org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty2;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;

import java.util.Optional;
import java.util.function.BiConsumer;

/** {@link ConsumerNettyService} is used to consume buffer from netty client in consumer side. */
public interface ConsumerNettyService {

    /**
     * Set up the netty service in consumer side.
     *
     * @param inputChannels in consumer side.
     * @param lastPrioritySequenceNumber is the array to record the priority sequence number.
     * @param subpartitionAvailableNotifier is used to notify the subpartition is available.
     */
    void setup(
            InputChannel[] inputChannels,
            int[] lastPrioritySequenceNumber,
            BiConsumer<Integer, Boolean> subpartitionAvailableNotifier);

    /**
     * Read a buffer related to the specific subpartition from NettyService.
     *
     * @param subpartitionId indicate the subpartition.
     * @return a buffer.
     */
    Optional<Buffer> readBuffer(int subpartitionId);

    /**
     * Notify that the data responding to a subpartition is available.
     *
     * @param subpartitionId indicate the subpartition.
     * @param priority indicate that if the subpartition is priority.
     */
    void notifyResultSubpartitionAvailable(int subpartitionId, boolean priority);

    /**
     * Notify that the specific segment is required according to the subpartitionId and segmentId.
     *
     * @param subpartitionId indicate the subpartition.
     * @param segmentId indicate the id of segment.
     */
    void notifyRequiredSegmentId(int subpartitionId, int segmentId);
}
