package org.apache.flink.runtime.io.network.partition.tieredstore.downstream;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.memory.MemorySegmentProvider;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreMemoryManager;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/** The factory of {@link TierReader}. */
public class TierReaderFactory {

    private final JobID jobID;

    private final List<ResultPartitionID> resultPartitionIDs;

    private final String baseRemoteStoragePath;

    private final List<Integer> subpartitionIndexes;

    private final boolean enableRemoteTier;

    private RemoteTierMonitor remoteTierMonitor;

    private TieredStoreMemoryManager memoryManager;

    public TierReaderFactory(
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
        if (enableRemoteTier) {
            this.memoryManager =
                    new DownstreamTieredStoreMemoryManager(
                            (NetworkBufferPool) memorySegmentProvider);
        }
    }

    public void setup(InputChannel[] channels, Consumer<InputChannel> channelEnqueueReceiver) {
        if (enableRemoteTier) {
            this.remoteTierMonitor =
                    new RemoteTierMonitor(
                            jobID, resultPartitionIDs, baseRemoteStoragePath, subpartitionIndexes);
            this.remoteTierMonitor.setup(channels, channelEnqueueReceiver);
        }
    }

    public List<TierReader> createClientList() {
        List<TierReader> clientList = new ArrayList<>();
        if (enableRemoteTier) {
            clientList.add(new LocalTierReader());
            clientList.add(new RemoteTierReader(memoryManager, remoteTierMonitor));
        } else {
            clientList.add(new LocalTierReader());
        }
        return clientList;
    }

    public boolean isEnableRemoteTier() {
        return enableRemoteTier;
    }
}
