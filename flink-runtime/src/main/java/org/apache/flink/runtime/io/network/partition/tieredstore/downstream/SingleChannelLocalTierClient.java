package org.apache.flink.runtime.io.network.partition.tieredstore.downstream;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.LocalRecoveredInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteRecoveredInputChannel;
import org.apache.flink.runtime.io.network.partition.tieredstore.downstream.common.SingleChannelTierClient;

import java.io.IOException;
import java.util.Optional;

/** The data client is used to fetch data from Local tier. */
public class SingleChannelLocalTierClient implements SingleChannelTierClient {

    private int latestSegmentId = 0;

    private boolean hasRegistered = false;

    @Override
    public Optional<InputChannel.BufferAndAvailability> getNextBuffer(
            InputChannel inputChannel, int segmentId) throws IOException, InterruptedException {
        if (inputChannel.getClass() == RemoteRecoveredInputChannel.class
                || inputChannel.getClass() == LocalRecoveredInputChannel.class) {
            return inputChannel.getNextBuffer();
        }
        if (segmentId > 0L && (segmentId != latestSegmentId || !hasRegistered)) {
            latestSegmentId = segmentId;
            inputChannel.notifyRequiredSegmentId(segmentId);
        }
        Optional<InputChannel.BufferAndAvailability> buffer;
        buffer = inputChannel.getNextBuffer();
        if (!hasRegistered && buffer.isPresent()) {
            hasRegistered = true;
        }
        return buffer;
    }

    @Override
    public void close() throws IOException {
        // nothing to do.
    }

    @VisibleForTesting
    public int getLatestSegmentId() {
        return latestSegmentId;
    }

    @VisibleForTesting
    public boolean hasRegistered() {
        return hasRegistered;
    }
}
