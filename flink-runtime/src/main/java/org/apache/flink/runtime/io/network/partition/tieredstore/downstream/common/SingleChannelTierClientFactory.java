package org.apache.flink.runtime.io.network.partition.tieredstore.downstream.common;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.memory.MemorySegmentProvider;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.tieredstore.downstream.SingleChannelRemoteTierClient;
import org.apache.flink.runtime.io.network.partition.tieredstore.downstream.SingleChannelLocalTierClient;

import java.util.ArrayList;
import java.util.List;

/** The factory of {@link SingleChannelTierClient}. */
public class SingleChannelTierClientFactory {

    private final JobID jobID;

    private final List<ResultPartitionID> resultPartitionIDs;

    private final String baseDfsPath;

    private final NetworkBufferPool networkBufferPool;

    private final int subpartitionIndex;

    private final boolean enableRemoteTier;

    public SingleChannelTierClientFactory(
            JobID jobID,
            List<ResultPartitionID> resultPartitionIDs,
            MemorySegmentProvider memorySegmentProvider,
            int subpartitionIndex,
            String baseDfsPath) {
        this.jobID = jobID;
        this.resultPartitionIDs = resultPartitionIDs;
        this.subpartitionIndex = subpartitionIndex;
        this.baseDfsPath = baseDfsPath;
        this.enableRemoteTier = baseDfsPath != null;
        this.networkBufferPool = (NetworkBufferPool) memorySegmentProvider;
    }

    public List<SingleChannelTierClient> createClientList() {
        List<SingleChannelTierClient> clientList = new ArrayList<>();
        if (enableRemoteTier) {
            clientList.add(new SingleChannelLocalTierClient());
            clientList.add(
                    new SingleChannelRemoteTierClient(
                            jobID,
                            resultPartitionIDs,
                            subpartitionIndex,
                            networkBufferPool,
                            baseDfsPath));
        } else {
            clientList.add(new SingleChannelLocalTierClient());
        }
        return clientList;
    }

    public boolean isEnableRemoteTier() {
        return enableRemoteTier;
    }
}
