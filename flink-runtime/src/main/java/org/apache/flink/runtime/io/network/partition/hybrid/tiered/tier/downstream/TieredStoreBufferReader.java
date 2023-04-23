package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.downstream;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.LocalRecoveredInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteRecoveredInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingInputGateBufferReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.common.TierMemorySpec;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.common.TieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.common.TieredStorageMemoryManagerImpl;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

/** The implementation of {@link TieredStorageConsumerClient} interface. */
public class TieredStoreBufferReader implements SingInputGateBufferReader {

    private final SubConsumerClient[] subConsumerClients;

    private final TieredStorageMemoryManager tieredStoreMemoryManager;

    private final String baseRemoteStoragePath;

    private final NetworkBufferPool networkBufferPool;

    private List<TierMemorySpec> tierMemorySpecs =
            new ArrayList<TierMemorySpec>() {
                {
                    add(new TierMemorySpec(0, 1, false));
                }
            };

    private RemoteTierMonitor remoteTierMonitor;

    public TieredStoreBufferReader(
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
        this.tieredStoreMemoryManager = new TieredStorageMemoryManagerImpl(tierMemorySpecs);
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
        this.subConsumerClients = new SubConsumerClient[numInputChannels];
        for (int i = 0; i < numInputChannels; ++i) {
            subConsumerClients[i] =
                    new SubConsumerClientImpl(getClientList(), channelEnqueueReceiver);
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

        return subConsumerClients[inputChannel.getChannelIndex()].getNextBuffer(inputChannel);
    }

    @Override
    public void close() throws IOException {
        for (SubConsumerClient subConsumerClient : subConsumerClients) {
            subConsumerClient.close();
        }
    }

    @Override
    public boolean supportAcknowledgeUpstreamAllRecordsProcessed() {
        return false;
    }

    private List<TierConsumerAgent> getClientList() {
        List<TierConsumerAgent> clientList = new ArrayList<>();
        if (baseRemoteStoragePath != null) {
            clientList.add(new LocalTierConsumerAgent());
            clientList.add(
                    new RemoteTierConsumerAgent(tieredStoreMemoryManager, remoteTierMonitor));
        } else {
            clientList.add(new LocalTierConsumerAgent());
        }
        return clientList;
    }
}
