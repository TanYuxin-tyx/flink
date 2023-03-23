package org.apache.flink.runtime.io.network.partition.tieredstore.downstream.common;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.memory.MemorySegmentProvider;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.tieredstore.downstream.RemoteTierMonitor;
import org.apache.flink.runtime.io.network.partition.tieredstore.downstream.SingleChannelLocalTierClient;
import org.apache.flink.runtime.io.network.partition.tieredstore.downstream.SingleChannelRemoteTierClient;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreMemoryManager;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/** The factory of {@link SingleChannelTierClient}. */
public class SingleChannelTierClientFactory {

    private final JobID jobID;

    private final List<ResultPartitionID> resultPartitionIDs;

    private final String baseRemoteStoragePath;

    private final TieredStoreMemoryManager memoryManager;

    private final List<Integer> subpartitionIndexes;

    private final boolean enableRemoteTier;

    private RemoteTierMonitor remoteTierMonitor;

    public SingleChannelTierClientFactory(
            JobID jobID,
            List<ResultPartitionID> resultPartitionIDs,
            MemorySegmentProvider memorySegmentProvider,
            List<Integer> subpartitionIndexes,
            String baseRemoteStoragePath) {
        this.jobID = jobID;
        this.resultPartitionIDs = resultPartitionIDs;
        this.subpartitionIndexes = subpartitionIndexes;
        this.baseRemoteStoragePath = baseRemoteStoragePath;
        this.enableRemoteTier = baseRemoteStoragePath != null;
        this.memoryManager =
                new DownstreamTieredStoreMemoryManager((NetworkBufferPool) memorySegmentProvider);
    }

    public void setup(InputChannel[] channels, Consumer<InputChannel> channelEnqueueReceiver) {
        if (enableRemoteTier) {
            this.remoteTierMonitor =
                    new RemoteTierMonitor(
                            jobID, resultPartitionIDs, baseRemoteStoragePath, subpartitionIndexes);
            this.remoteTierMonitor.setup(channels, channelEnqueueReceiver);
        }
    }

    public List<SingleChannelTierClient> createClientList() {
        List<SingleChannelTierClient> clientList = new ArrayList<>();
        if (enableRemoteTier) {
            clientList.add(new SingleChannelLocalTierClient());
            clientList.add(new SingleChannelRemoteTierClient(memoryManager, remoteTierMonitor));
        } else {
            clientList.add(new SingleChannelLocalTierClient());
        }
        return clientList;
    }

    public boolean isEnableRemoteTier() {
        return enableRemoteTier;
    }
}
