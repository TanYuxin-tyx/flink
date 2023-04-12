package org.apache.flink.runtime.io.network.partition.hybrid.tiered.downstream;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.memory.MemorySegmentProvider;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoreMemoryManager;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/** The factory of {@link TierStorageClient}. */
public class TierReaderFactoryImpl implements TierReaderFactory {

    private final JobID jobID;

    private final List<ResultPartitionID> resultPartitionIDs;

    private final String baseRemoteStoragePath;

    private final List<Integer> subpartitionIndexes;

    private final boolean enableRemoteTier;

    private RemoteTierMonitor remoteTierMonitor;

    private TieredStoreMemoryManager memoryManager;

    public TierReaderFactoryImpl(
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
    }

    @Override
    public void start() {
        if (remoteTierMonitor != null) {
            this.remoteTierMonitor.start();
        }
    }

    public List<TierStorageClient> createClientList() {
        List<TierStorageClient> clientList = new ArrayList<>();
        if (enableRemoteTier) {
            clientList.add(new LocalTierStorageClient());
            clientList.add(new RemoteTierStorageClient(memoryManager, remoteTierMonitor));
        } else {
            clientList.add(new LocalTierStorageClient());
        }
        return clientList;
    }

    public boolean isEnableRemoteTier() {
        return enableRemoteTier;
    }
}
