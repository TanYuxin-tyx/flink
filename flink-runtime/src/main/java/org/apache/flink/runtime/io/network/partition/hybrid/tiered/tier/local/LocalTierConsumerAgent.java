package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.local;

import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierConsumerAgent;

import java.io.IOException;
import java.util.Optional;

/** The data client is used to fetch data from Local tier. */
public class LocalTierConsumerAgent implements TierConsumerAgent {

    private final SubLocalTierConsumerAgent[] subAgents;

    public LocalTierConsumerAgent(int numInputChannels) {
        this.subAgents = new SubLocalTierConsumerAgent[numInputChannels];
        for (int index = 0; index < numInputChannels; ++index) {
            this.subAgents[index] = new SubLocalTierConsumerAgent();
        }
    }

    @Override
    public void start() {
        // nothing to do.
    }

    @Override
    public Optional<InputChannel.BufferAndAvailability> getNextBuffer(
            InputChannel inputChannel, int segmentId) throws IOException, InterruptedException {
        return subAgents[inputChannel.getChannelIndex()].getNextBuffer(inputChannel, segmentId);
    }

    @Override
    public void close() throws IOException {
        // nothing to do.
    }

    private static class SubLocalTierConsumerAgent {

        private int latestSegmentId = 0;

        private SubLocalTierConsumerAgent() {}

        public Optional<InputChannel.BufferAndAvailability> getNextBuffer(
                InputChannel inputChannel, int segmentId) throws IOException, InterruptedException {
            if (segmentId > 0L && (segmentId != latestSegmentId)) {
                latestSegmentId = segmentId;
                inputChannel.notifyRequiredSegmentId(segmentId);
            }
            Optional<InputChannel.BufferAndAvailability> buffer;
            buffer = inputChannel.getNextBuffer();
            return buffer;
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
