package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.LocalRecoveredInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteRecoveredInputChannel;
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
import java.util.function.Consumer;

/**
 * The implementation of {@link
 * org.apache.flink.runtime.io.network.partition.consumer.TieredStorageConsumerClient} interface.
 */
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
            Consumer<Integer> channelEnqueueReceiver) {
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
                        channelEnqueueReceiver);
        this.subpartitionConsumerClients = new SubpartitionConsumerClient[numInputChannels];
        for (int i = 0; i < numInputChannels; ++i) {
            subpartitionConsumerClients[i] =
                    new SubpartitionConsumerClientImpl(tierConsumerAgents, channelEnqueueReceiver);
        }
    }

    public void start() {
        for (TierConsumerAgent tierConsumerAgent : tierConsumerAgents) {
            tierConsumerAgent.start();
        }
    }

    public Optional<InputChannel.BufferAndAvailability> getNextBuffer(InputChannel inputChannel)
            throws IOException, InterruptedException {

        if (inputChannel.getClass() == LocalRecoveredInputChannel.class
                || inputChannel.getClass() == RemoteRecoveredInputChannel.class) {
            return inputChannel.getNextBuffer();
        }

        return subpartitionConsumerClients[inputChannel.getChannelIndex()].getNextBuffer(
                inputChannel);
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
            Consumer<Integer> channelEnqueueReceiver) {
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
                            channelEnqueueReceiver));
        }
        return new ArrayList<>(tierConsumerAgents);
    }
}
