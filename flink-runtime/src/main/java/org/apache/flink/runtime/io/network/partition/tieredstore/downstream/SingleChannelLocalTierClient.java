package org.apache.flink.runtime.io.network.partition.tieredstore.downstream;

import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.LocalRecoveredInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteRecoveredInputChannel;
import org.apache.flink.runtime.io.network.partition.tieredstore.downstream.common.SingleChannelTierClient;

import java.io.IOException;
import java.util.Optional;

/** The data client is used to fetch data from Local tier. */
public class SingleChannelLocalTierClient implements SingleChannelTierClient {

    private long latestSegmentId = 0;

    @Override
    public Optional<InputChannel.BufferAndAvailability> getNextBuffer(
            InputChannel inputChannel, long segmentId) throws IOException, InterruptedException {
        if (inputChannel.getClass() == RemoteRecoveredInputChannel.class
                || inputChannel.getClass() == LocalRecoveredInputChannel.class) {
            return inputChannel.getNextBuffer();
        }
        if (segmentId > 0L && segmentId != latestSegmentId) {
            latestSegmentId = segmentId;
            inputChannel.notifyRequiredSegmentId(segmentId);
        }
        return inputChannel.getNextBuffer();
    }

    // @Override
    // public boolean hasSegmentId(InputChannel inputChannel, long segmentId) {
    //    checkState(
    //            segmentId >= latestSegmentId,
    //            "The segmentId is illegal, current: %s, latest: %s",
    //            segmentId,
    //            latestSegmentId);
    //    if (segmentId > latestSegmentId) {
    //        inputChannel.notifyRequiredSegmentId(segmentId);
    //    }
    //    latestSegmentId = segmentId;
    //    return inputChannel.containSegment(segmentId);
    // }

    @Override
    public void close() throws IOException {
        // nothing to do.
    }
}
