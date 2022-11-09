package org.apache.flink.runtime.io.network.partition.consumer.tier;

import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.LocalRecoveredInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteRecoveredInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.tier.common.SingleChannelDataClient;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkState;

/** The data client is used to fetch data from Local tier. */
public class SingleChannelLocalDataClient implements SingleChannelDataClient {

    private long latestSegmentId = 0;

    @Override
    public boolean hasSegmentId(InputChannel inputChannel, long segmentId) {
        checkState(
                segmentId >= latestSegmentId,
                "The segmentId is illegal, current: %s, latest: %s",
                segmentId,
                latestSegmentId);
        if (segmentId > latestSegmentId) {
            inputChannel.notifyRequiredSegmentId(segmentId);
        }
        latestSegmentId = segmentId;
        if (inputChannel.containSegment(segmentId)) {
            return true;
        }
        return false;
    }

    private int sleepChannel = -1;

    @Override
    public Optional<InputChannel.BufferAndAvailability> getNextBuffer(
            InputChannel inputChannel, long segmentId) throws IOException, InterruptedException {
        if (inputChannel.getClass() == RemoteRecoveredInputChannel.class
                || inputChannel.getClass() == LocalRecoveredInputChannel.class) {
            return inputChannel.getNextBuffer();
        }
        if (segmentId != latestSegmentId) {
            sleepChannel = inputChannel.getChannelIndex();
            TimeUnit.SECONDS.sleep(100000000);
        }
        checkState(
                segmentId == latestSegmentId,
                "The segmentId is illegal, current: %s, latest: %s",
                segmentId,
                latestSegmentId);
        checkState(inputChannel.containSegment(segmentId));
        return inputChannel.getNextBuffer();
    }

    @Override
    public void close() throws IOException {
        // nothing to do.
    }
}
