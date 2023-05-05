package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage;

import org.apache.flink.runtime.io.network.buffer.Buffer;
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

    public Optional<Buffer> getNextBuffer(int subpartitionId)
            throws IOException, InterruptedException {
        Optional<Buffer> bufferAndAvailability = Optional.empty();
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
        Buffer bufferData = bufferAndAvailability.get();
        if (bufferData.getDataType() == Buffer.DataType.ADD_SEGMENT_ID_EVENT) {
            currentSegmentId++;
            bufferData.recycleBuffer();
            consumerNettyService.notifyResultSubpartitionAvailable(subpartitionId, false);
            return getNextBuffer(subpartitionId);
        }
        return Optional.of(bufferData);
    }
}
