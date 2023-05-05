package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.local;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierConsumerAgent;

import java.io.IOException;
import java.util.Optional;

/** The data client is used to fetch data from Local tier. */
public class LocalTierConsumerAgent implements TierConsumerAgent {

    private final SubLocalTierConsumerAgent[] subAgents;

    public LocalTierConsumerAgent(int numInputChannels, NettyService consumerNettyService) {
        this.subAgents = new SubLocalTierConsumerAgent[numInputChannels];
        for (int index = 0; index < numInputChannels; ++index) {
            this.subAgents[index] = new SubLocalTierConsumerAgent(consumerNettyService);
        }
    }

    @Override
    public void start() {
        // nothing to do.
    }

    @Override
    public Optional<Buffer> getNextBuffer(
            int subpartitionId, int segmentId) throws IOException, InterruptedException {
        return subAgents[subpartitionId].getNextBuffer(subpartitionId, segmentId);
    }

    @Override
    public void close() throws IOException {
        // nothing to do.
    }

    private static class SubLocalTierConsumerAgent {

        private final NettyService consumerNettyService;

        private int latestSegmentId = 0;

        private SubLocalTierConsumerAgent(NettyService consumerNettyService) {
            this.consumerNettyService = consumerNettyService;
        }

        public Optional<Buffer> getNextBuffer(
                int subpartitionId, int segmentId) throws IOException, InterruptedException {
            if (segmentId > 0L && (segmentId != latestSegmentId)) {
                latestSegmentId = segmentId;
                consumerNettyService.notifyRequiredSegmentId(subpartitionId, segmentId);
            }
            return consumerNettyService.readBuffer(subpartitionId);
        }
    }

    @Override
    public boolean equals(Object o) {
        return o.getClass() == LocalTierConsumerAgent.class;
    }

    @Override
    public int hashCode() {
        return 0;
    }
}
