package org.apache.flink.runtime.io.network.partition.consumer.tier.common;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.memory.MemorySegmentProvider;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.tier.SingleChannelDfsDataClient;
import org.apache.flink.runtime.io.network.partition.consumer.tier.SingleChannelLocalDataClient;
import org.apache.flink.runtime.io.network.partition.consumer.tier.history.DfsFetcherDataQueue;
import org.apache.flink.runtime.io.network.partition.consumer.tier.history.FetcherDataQueue;

import java.util.List;

/** The factory of {@link SingleChannelDataClient}. */
public class SingleChannelDataClientFactory {

    private final FetcherDataQueue fetcherDataQueue = new DfsFetcherDataQueue();

    private final JobID jobID;

    private final List<ResultPartitionID> resultPartitionIDs;

    private final String baseDfsPath;

    private final SingleInputGate singleInputGate;

    private final NetworkBufferPool networkBufferPool;

    private final int subpartitionIndex;

    private final boolean hasDfsClient;

    public SingleChannelDataClientFactory(
            SingleInputGate singleInputGate,
            JobID jobID,
            List<ResultPartitionID> resultPartitionIDs,
            MemorySegmentProvider memorySegmentProvider,
            int subpartitionIndex,
            String baseDfsPath) {
        this.singleInputGate = singleInputGate;
        this.jobID = jobID;
        this.resultPartitionIDs = resultPartitionIDs;
        this.subpartitionIndex = subpartitionIndex;
        this.baseDfsPath = baseDfsPath;
        this.hasDfsClient = baseDfsPath != null;
        this.networkBufferPool = (NetworkBufferPool) memorySegmentProvider;
    }

    public SingleChannelDataClient createLocalSingleChannelDataClient() {
        return new SingleChannelLocalDataClient();
    }

    public SingleChannelDataClient createDfsSingleChannelDataClient() {
        if (!hasDfsClient) {
            return null;
        }
        return new SingleChannelDfsDataClient(
                jobID, resultPartitionIDs, subpartitionIndex, networkBufferPool, baseDfsPath);
    }

    public boolean hasDfsClient() {
        return hasDfsClient;
    }
}
