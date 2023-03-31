package org.apache.flink.runtime.io.network.partition.tieredstore.downstream;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.LocalRecoveredInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteRecoveredInputChannel;

import java.io.IOException;
import java.util.Optional;

/** The data client is used to fetch data from Local tier. */
public class LocalTierClient implements TierClient {

    private int latestSegmentId = 0;

    @Override
    public Optional<InputChannel.BufferAndAvailability> getNextBuffer(
            InputChannel inputChannel, int segmentId) throws IOException, InterruptedException {
        if (inputChannel.getClass() == RemoteRecoveredInputChannel.class
                || inputChannel.getClass() == LocalRecoveredInputChannel.class) {
            return inputChannel.getNextBuffer();
        }
        if (segmentId > 0L && (segmentId != latestSegmentId)) {
            latestSegmentId = segmentId;
            inputChannel.notifyRequiredSegmentId(segmentId);
        }
        Optional<InputChannel.BufferAndAvailability> buffer;
        buffer = inputChannel.getNextBuffer();
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
}
