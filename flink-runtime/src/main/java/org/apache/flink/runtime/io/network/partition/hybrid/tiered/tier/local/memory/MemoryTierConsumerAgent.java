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
    private final NettyServiceReader nettyServiceReader;

    public MemoryTierConsumerAgent(NettyServiceReaderId readerId, TieredStorageNettyService nettyService) {
        this.nettyServiceReader = nettyService.registerConsumer(readerId);
    }

    @Override
    public void start() {
        // nothing to do.
    }

    @Override
    public Optional<Buffer> getNextBuffer(int subpartitionId, int segmentId) {
        return nettyServiceReader.readBuffer(subpartitionId, segmentId);
    }

    @Override
    public void close() throws IOException {
        // nothing to do.
    }
}
