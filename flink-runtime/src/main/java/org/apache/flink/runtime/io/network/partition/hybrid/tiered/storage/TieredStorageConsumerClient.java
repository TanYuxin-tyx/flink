package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.LocalRecoveredInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteRecoveredInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingInputGateConsumerClient;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.IndexedTierConfSpec;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageConfiguration;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierConsumerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.local.LocalTierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.remote.RemoteTierConsumerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.remote.RemoteTierMonitor;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

/** The implementation of {@link SingInputGateConsumerClient} interface. */
public class TieredStorageConsumerClient implements SingInputGateConsumerClient {

    private final SubpartitionConsumerClient[] subpartitionConsumerClients;

    private final TieredStorageMemoryManager tieredStoreMemoryManager;

    private final String baseRemoteStoragePath;

    private final NetworkBufferPool networkBufferPool;

    private final LocalTierFactory localTierFactory = new LocalTierFactory();

    private RemoteTierMonitor remoteTierMonitor;

    public TieredStorageConsumerClient(
            boolean isUpstreamBroadcast,
            int numInputChannels,
            JobID jobID,
            List<ResultPartitionID> resultPartitionIDs,
            NetworkBufferPool networkBufferPool,
            List<Integer> subpartitionIndexes,
            String baseRemoteStoragePath,
            Consumer<Integer> channelEnqueueReceiver) {
        this.baseRemoteStoragePath = baseRemoteStoragePath;
        this.networkBufferPool = networkBufferPool;
        List<IndexedTierConfSpec> indexedTierConfSpecs = TieredStorageConfiguration.getTestIndexedTierConfSpec();
        this.tieredStoreMemoryManager = new TieredStorageMemoryManagerImpl(indexedTierConfSpecs);
        if (baseRemoteStoragePath != null) {
            this.remoteTierMonitor =
                    new RemoteTierMonitor(
                            jobID,
                            resultPartitionIDs,
                            baseRemoteStoragePath,
                            subpartitionIndexes,
                            numInputChannels,
                            isUpstreamBroadcast,
                            channelEnqueueReceiver);
        }
        this.subpartitionConsumerClients = new SubpartitionConsumerClient[numInputChannels];
        for (int i = 0; i < numInputChannels; ++i) {
            subpartitionConsumerClients[i] =
                    new SubpartitionConsumerClientImpl(getClientList(), channelEnqueueReceiver);
        }
    }

    @Override
    public void start() {
        try {
            this.tieredStoreMemoryManager.setup(networkBufferPool.createBufferPool(1, 1));
        } catch (IOException e) {
            ExceptionUtils.rethrow(e, "Failed to start.");
        }
        if (baseRemoteStoragePath != null) {
            this.remoteTierMonitor.start();
        }
    }

    @Override
    public Optional<InputChannel.BufferAndAvailability> getNextBuffer(InputChannel inputChannel)
            throws IOException, InterruptedException {

        if (inputChannel.getClass() == LocalRecoveredInputChannel.class
                || inputChannel.getClass() == RemoteRecoveredInputChannel.class) {
            return inputChannel.getNextBuffer();
        }

        return subpartitionConsumerClients[inputChannel.getChannelIndex()].getNextBuffer(
                inputChannel);
    }

    @Override
    public void close() throws IOException {
        for (SubpartitionConsumerClient subpartitionConsumerClient : subpartitionConsumerClients) {
            subpartitionConsumerClient.close();
        }
    }

    @Override
    public boolean supportAcknowledgeUpstreamAllRecordsProcessed() {
        return false;
    }

    private List<TierConsumerAgent> getClientList() {
        List<TierConsumerAgent> clientList = new ArrayList<>();
        if (baseRemoteStoragePath != null) {
            clientList.add(localTierFactory.createConsumerAgent());
            clientList.add(
                    new RemoteTierConsumerAgent(tieredStoreMemoryManager, remoteTierMonitor));
        } else {
            clientList.add(localTierFactory.createConsumerAgent());
        }
        return clientList;
    }
}
