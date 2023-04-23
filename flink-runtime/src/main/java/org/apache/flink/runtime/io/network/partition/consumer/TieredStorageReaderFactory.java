package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.downstream.TieredStoreConsumerClient;

import java.util.List;
import java.util.function.Consumer;

/** {@link TieredStorageReaderFactory} is the factory to create reader of tiered storage. */
public class TieredStorageReaderFactory {

    private final boolean enableTieredStorage;

    private final boolean isUpstreamBroadcast;

    private final int numInputChannels;

    private final JobID jobID;

    private final List<ResultPartitionID> resultPartitionIDs;

    private final NetworkBufferPool networkBufferPool;

    private final List<Integer> subpartitionIndexes;

    private final String baseRemoteStoragePath;

    public TieredStorageReaderFactory(
            boolean enableTieredStorage,
            boolean isUpstreamBroadcast,
            int numInputChannels,
            JobID jobID,
            List<ResultPartitionID> resultPartitionIDs,
            NetworkBufferPool networkBufferPool,
            List<Integer> subpartitionIndexes,
            String baseRemoteStoragePath) {
        this.enableTieredStorage = enableTieredStorage;
        this.isUpstreamBroadcast = isUpstreamBroadcast;
        this.numInputChannels = numInputChannels;
        this.jobID = jobID;
        this.resultPartitionIDs = resultPartitionIDs;
        this.networkBufferPool = networkBufferPool;
        this.subpartitionIndexes = subpartitionIndexes;
        this.baseRemoteStoragePath = baseRemoteStoragePath;
    }

    public SingInputGateConsumerClient createSingInputGateBufferReader(
            Consumer<Integer> channelEnqueueReceiver) {
        if (enableTieredStorage) {
            return new TieredStoreConsumerClient(
                    isUpstreamBroadcast,
                    numInputChannels,
                    jobID,
                    resultPartitionIDs,
                    networkBufferPool,
                    subpartitionIndexes,
                    baseRemoteStoragePath,
                    channelEnqueueReceiver);
        }
        return new DefaultConsumerClient();
    }
}
