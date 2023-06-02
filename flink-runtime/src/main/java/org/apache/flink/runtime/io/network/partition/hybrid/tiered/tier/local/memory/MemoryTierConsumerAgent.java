package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.local.memory;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyServiceReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyServiceReaderId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierConsumerAgent;

import java.io.IOException;
import java.util.Optional;

/** The data client is used to fetch data from memory tier. */
public class MemoryTierConsumerAgent implements TierConsumerAgent {

    private final int[] requiredSegmentIds;

    private final NettyServiceReader consumerNettyService;

    public MemoryTierConsumerAgent(int[] requiredSegmentIds, NettyServiceReaderId readerId,
                                   TieredStorageNettyService nettyService) {
        this.requiredSegmentIds = requiredSegmentIds;
        this.consumerNettyService = nettyService.registerConsumer(readerId);
    }

    @Override
    public void start() {
        // nothing to do.
    }

    @Override
    public Optional<Buffer> getNextBuffer(int subpartitionId, int segmentId) {
        if (segmentId > 0L && (segmentId != requiredSegmentIds[subpartitionId])) {
            requiredSegmentIds[subpartitionId] = segmentId;
            consumerNettyService.notifyRequiredSegmentId(subpartitionId, segmentId);
        }
        return consumerNettyService.readBuffer(subpartitionId);
    }

    @Override
    public void close() throws IOException {
        // nothing to do.
    }
}
