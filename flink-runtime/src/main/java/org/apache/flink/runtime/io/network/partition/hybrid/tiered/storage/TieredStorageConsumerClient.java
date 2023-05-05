package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierConsumerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.local.disk.DiskTierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.local.memory.MemoryTierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.remote.RemoteTierFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** The implementation of interface. */
public class TieredStorageConsumerClient {

    private final SubpartitionConsumerClient[] subpartitionConsumerClients;

    private final List<TierFactory> tierFactories;

    private final List<TierConsumerAgent> tierConsumerAgents;

    public TieredStorageConsumerClient(
            boolean isUpstreamBroadcast,
            int numInputChannels,
            JobID jobID,
            List<ResultPartitionID> resultPartitionIDs,
            NetworkBufferPool networkBufferPool,
            List<Integer> subpartitionIndexes,
            String baseRemoteStoragePath,
            NettyService consumerNettyService) {
        this.tierFactories = createTierFactories(baseRemoteStoragePath);
        this.tierConsumerAgents =
                createTierConsumerAgents(
                        numInputChannels,
                        isUpstreamBroadcast,
                        jobID,
                        resultPartitionIDs,
                        networkBufferPool,
                        subpartitionIndexes,
                        baseRemoteStoragePath,
                        consumerNettyService);
        this.subpartitionConsumerClients = new SubpartitionConsumerClient[numInputChannels];
        for (int i = 0; i < numInputChannels; ++i) {
            subpartitionConsumerClients[i] =
                    new SubpartitionConsumerClient(tierConsumerAgents, consumerNettyService);
        }
    }

    public void start() {
        for (TierConsumerAgent tierConsumerAgent : tierConsumerAgents) {
            tierConsumerAgent.start();
        }
    }

    public Optional<InputChannel.BufferAndAvailability> getNextBuffer(int subpartitionId)
            throws IOException, InterruptedException {

        // if (inputChannel.getClass() == LocalRecoveredInputChannel.class
        //        || inputChannel.getClass() == RemoteRecoveredInputChannel.class) {
        //    return inputChannel.getNextBuffer();
        // }

        return subpartitionConsumerClients[subpartitionId].getNextBuffer(subpartitionId);
    }

    public void close() throws IOException {
        for (TierConsumerAgent tierConsumerAgent : tierConsumerAgents) {
            tierConsumerAgent.close();
        }
    }

    private List<TierFactory> createTierFactories(String baseRemoteStoragePath) {
        List<TierFactory> tierFactories = new ArrayList<>();
        tierFactories.add(new MemoryTierFactory());
        tierFactories.add(new DiskTierFactory());
        if (baseRemoteStoragePath != null) {
            tierFactories.add(new RemoteTierFactory());
        }
        return tierFactories;
    }

    private List<TierConsumerAgent> createTierConsumerAgents(
            int numInputChannels,
            boolean isUpstreamBroadcastOnly,
            JobID jobID,
            List<ResultPartitionID> resultPartitionIDs,
            NetworkBufferPool networkBufferPool,
            List<Integer> subpartitionIndexes,
            String baseRemoteStoragePath,
            NettyService consumerNettyService) {
        Set<TierConsumerAgent> tierConsumerAgents = new HashSet<>();
        for (TierFactory tierFactory : tierFactories) {
            tierConsumerAgents.add(
                    tierFactory.createConsumerAgent(
                            isUpstreamBroadcastOnly,
                            numInputChannels,
                            jobID,
                            resultPartitionIDs,
                            networkBufferPool,
                            subpartitionIndexes,
                            baseRemoteStoragePath,
                            consumerNettyService));
        }
        return new ArrayList<>(tierConsumerAgents);
    }
}
