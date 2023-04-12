package org.apache.flink.runtime.io.network.partition.hybrid.tiered.downstream;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.LocalRecoveredInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteRecoveredInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingInputGateBufferReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoreMemoryManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

/** The implementation of {@link TieredStoreReader} interface. */
public class TieredStoreBufferReader implements SingInputGateBufferReader {

    private final SubpartitionReader[] subpartitionReaders;

    private final TieredStoreMemoryManager tieredStoreMemoryManager;

    private final String baseRemoteStoragePath;

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
        this.tieredStoreMemoryManager = new DownstreamTieredStoreMemoryManager(networkBufferPool);
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
        this.subpartitionReaders = new SubpartitionReader[numInputChannels];
        for (int i = 0; i < numInputChannels; ++i) {
            subpartitionReaders[i] =
                    new SubpartitionReaderImpl(getClientList(), channelEnqueueReceiver);
        }
    }

    @Override
    public void start() {
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

        return subpartitionReaders[inputChannel.getChannelIndex()].getNextBuffer(inputChannel);
    }

    @Override
    public void close() throws IOException {
        for (SubpartitionReader subpartitionReader : subpartitionReaders) {
            subpartitionReader.close();
        }
    }

    @Override
    public boolean supportAcknowledgeAllRecordsProcessed() {
        return false;
    }

    private List<TierStorageClient> getClientList() {
        List<TierStorageClient> clientList = new ArrayList<>();
        if (baseRemoteStoragePath != null) {
            clientList.add(new LocalTierStorageClient());
            clientList.add(
                    new RemoteTierStorageClient(tieredStoreMemoryManager, remoteTierMonitor));
        } else {
            clientList.add(new LocalTierStorageClient());
        }
        return clientList;
    }
}
