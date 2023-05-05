package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierConsumerAgent;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/** The implementation of {@link SubpartitionConsumerClient} interface. */
public class SubpartitionConsumerClient {

    private final NettyService consumerNettyService;

    private final List<TierConsumerAgent> agentList;

    private int currentSegmentId = 0;

    public SubpartitionConsumerClient(
            List<TierConsumerAgent> agentList, NettyService consumerNettyService) {
        this.agentList = agentList;
        this.consumerNettyService = consumerNettyService;
    }

    public Optional<BufferAndAvailability> getNextBuffer(int subpartitionId)
            throws IOException, InterruptedException {
        Optional<BufferAndAvailability> bufferAndAvailability = Optional.empty();
        for (TierConsumerAgent tiereConsumerAgent : agentList) {
            bufferAndAvailability =
                    tiereConsumerAgent.getNextBuffer(subpartitionId, currentSegmentId);
            if (bufferAndAvailability.isPresent()) {
                break;
            }
        }
        if (!bufferAndAvailability.isPresent()) {
            return Optional.empty();
        }
        BufferAndAvailability bufferData = bufferAndAvailability.get();
        if (bufferData.buffer().getDataType() == Buffer.DataType.ADD_SEGMENT_ID_EVENT) {
            currentSegmentId++;
            bufferData.buffer().recycleBuffer();
            consumerNettyService.notifyResultSubpartitionAvailable(subpartitionId);
            return getNextBuffer(subpartitionId);
        }
        return Optional.of(bufferData);
    }
}
