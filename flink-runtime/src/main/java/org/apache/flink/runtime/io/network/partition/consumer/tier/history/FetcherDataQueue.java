package org.apache.flink.runtime.io.network.partition.consumer.tier.history;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/** The FetcherDataQueue is used in {@link DataFetcherClient}. */
public interface FetcherDataQueue {

    /** Set up the FetcherDataQueue. */
    void setup(
            JobID jobID,
            List<ResultPartitionID> resultPartitionIDs,
            BufferPool bufferPool,
            int subpartitionIndex,
            String baseDfsPath)
            throws InterruptedException, IOException;

    /** Close the FetcherDataQueue. */
    void close() throws IOException;

    /** Initialize the FetcherDataQueue with segment information. */
    void setSegmentInfo(InputChannel inputChannel, long segmentId)
            throws InterruptedException, IOException;

    /** Get the buffer in the blocking mode or non-blocking mode. */
    Optional<BufferAndAvailability> getNextBuffer(Boolean isBlocking) throws InterruptedException;

    /** Get the current state of FetcherDataQueue. */
    FetcherDataQueueState getState();

    /** Set the current state of FetcherDataQueue. */
    void setState(FetcherDataQueueState state);

    /** Get the current reading channel. */
    Optional<InputChannel> getCurrentChannel();

    /** Determine whether the segment belonged to the specific channel exists. */
    boolean isSegmentExist(long segmentId, InputChannel inputChannel);

    /** Get the current reading segment. */
    long getCurrentSegmentId(InputChannel inputChannel);
}
