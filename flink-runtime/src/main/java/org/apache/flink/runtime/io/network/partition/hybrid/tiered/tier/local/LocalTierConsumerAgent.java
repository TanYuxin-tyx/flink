package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.local;

import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierConsumerAgent;

import java.io.IOException;
import java.util.Optional;

/** The data client is used to fetch data from Local tier. */
public class LocalTierConsumerAgent implements TierConsumerAgent {

    private static LocalTierConsumerAgent instance = null;

    private final SubLocalTierConsumerAgent[] consumerAgents;

    private LocalTierConsumerAgent(int numInputChannels) {
        this.consumerAgents = new SubLocalTierConsumerAgent[numInputChannels];
        for (int index = 0; index < numInputChannels; ++index) {
            this.consumerAgents[index] = new SubLocalTierConsumerAgent();
        }
    }

    public static LocalTierConsumerAgent getInstance(int numInputChannels) {
        if (instance == null) {
            synchronized (LocalTierConsumerAgent.class) {
                if (instance == null) {
                    instance = new LocalTierConsumerAgent(numInputChannels);
                }
            }
        }
        return instance;
    }

    @Override
    public void start() {
        // nothing to do.
    }

    @Override
    public Optional<InputChannel.BufferAndAvailability> getNextBuffer(
            InputChannel inputChannel, int segmentId) throws IOException, InterruptedException {
        return consumerAgents[inputChannel.getChannelIndex()].getNextBuffer(
                inputChannel, segmentId);
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
}
