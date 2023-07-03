package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate.AvailabilityAndPriorityRetriever;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.PartitionFileReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierConsumerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.disk.DiskTierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.memory.MemoryTierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.remote.RemoteStorageScanner;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.remote.RemoteTierFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** {@link TieredStorageConsumerClient} is used to read buffer from tiered store. */
public class TieredStorageConsumerClient {

    private final List<TierFactory> tierFactories;

    private final List<TierConsumerAgent> tierConsumerAgents;

    /**
     * The key includes {@link TieredStoragePartitionId} and {@link TieredStorageSubpartitionId}.
     * The value includes the current consumer agent, segment number, and sequence number of buffer.
     */
    private final Map<
                    TieredStoragePartitionId,
                    Map<TieredStorageSubpartitionId, Tuple3<TierConsumerAgent, Integer, Integer>>>
            currentConsumerInfo = new HashMap<>();

    private final RemoteStorageScanner remoteStorageScanner;

    private AvailabilityAndPriorityRetriever retriever;

    public TieredStorageConsumerClient(
            List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs,
            TieredStorageNettyService nettyService,
            String remoteStorageBasePath,
            RemoteStorageScanner remoteStorageScanner,
            PartitionFileReader partitionFileReader,
            int remoteBufferSize) {
        this.tierFactories = createTierFactories(remoteStorageBasePath);
        this.remoteStorageScanner = remoteStorageScanner;
        this.tierConsumerAgents =
                createTierConsumerAgents(
                        tieredStorageConsumerSpecs,
                        nettyService,
                        remoteStorageScanner,
                        partitionFileReader,
                        remoteBufferSize);
    }

    public void start() {
        for (TierConsumerAgent tierConsumerAgent : tierConsumerAgents) {
            tierConsumerAgent.start();
        }
    }

    public Optional<Buffer> getNextBuffer(
            TieredStoragePartitionId partitionId, TieredStorageSubpartitionId subpartitionId) {
        Map<TieredStorageSubpartitionId, Tuple3<TierConsumerAgent, Integer, Integer>>
                subpartitionInfo =
                        currentConsumerInfo.computeIfAbsent(partitionId, ignore -> new HashMap<>());
        Tuple3<TierConsumerAgent, Integer, Integer> agentInfo =
                subpartitionInfo.getOrDefault(subpartitionId, Tuple3.of(null, 0, 0));
        Optional<Buffer> buffer = Optional.empty();
        if (agentInfo.f0 == null) {
            for (TierConsumerAgent tierConsumerAgent : tierConsumerAgents) {
                buffer = tierConsumerAgent.getNextBuffer(partitionId, subpartitionId, agentInfo.f1);
                if (buffer.isPresent()) {
                    agentInfo.setField(tierConsumerAgent, 0);
                    break;
                }
            }
        } else {
            buffer = agentInfo.f0.getNextBuffer(partitionId, subpartitionId, agentInfo.f1);
        }
        if (!buffer.isPresent()) {
            return Optional.empty();
        }
        Buffer bufferData = buffer.get();
        retriever.retrieveAvailableAndPriority(
                partitionId, subpartitionId, bufferData.getDataType().hasPriority(), agentInfo.f2);
        agentInfo.setField(agentInfo.f2 + 1, 2);
        subpartitionInfo.put(subpartitionId, agentInfo);
        if (bufferData.getDataType() == Buffer.DataType.END_OF_SEGMENT) {
            agentInfo.setField(null, 0);
            agentInfo.setField(agentInfo.f1 + 1, 1);
            subpartitionInfo.put(subpartitionId, agentInfo);
            bufferData.recycleBuffer();
            return getNextBuffer(partitionId, subpartitionId);
        }
        return Optional.of(bufferData);
    }

    public void registerAvailabilityAndPriorityRetriever(
            AvailabilityAndPriorityRetriever retriever) {
        this.retriever = retriever;
        if (remoteStorageScanner != null) {
            remoteStorageScanner.registerAvailabilityAndPriorityRetriever(retriever);
        }
    }

    public void close() throws IOException {
        for (TierConsumerAgent tierConsumerAgent : tierConsumerAgents) {
            tierConsumerAgent.close();
        }
    }

    private List<TierFactory> createTierFactories(String remoteStorageBasePath) {
        List<TierFactory> tierFactories = new ArrayList<>();
        tierFactories.add(new MemoryTierFactory(0, 0));
        tierFactories.add(new DiskTierFactory(0, 0, 0));
        if (remoteStorageBasePath != null) {
            tierFactories.add(new RemoteTierFactory(0, 0, remoteStorageBasePath));
        }
        return tierFactories;
    }

    private List<TierConsumerAgent> createTierConsumerAgents(
            List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs,
            TieredStorageNettyService nettyService,
            RemoteStorageScanner remoteStorageScanner,
            PartitionFileReader partitionFileReader,
            int remoteBufferSize) {
        List<TierConsumerAgent> tierConsumerAgents = new ArrayList<>();
        Map<
                        TieredStoragePartitionId,
                        Map<TieredStorageSubpartitionId, CompletableFuture<NettyConnectionReader>>>
                nettyConnectionReaders = new HashMap<>();
        for (TieredStorageConsumerSpec spec : tieredStorageConsumerSpecs) {
            TieredStoragePartitionId partitionId = spec.getPartitionId();
            TieredStorageSubpartitionId subpartitionId = spec.getSubpartitionId();
            nettyConnectionReaders
                    .computeIfAbsent(partitionId, ignore -> new HashMap<>())
                    .put(
                            subpartitionId,
                            nettyService.registerConsumer(partitionId, subpartitionId));
        }

        for (TierFactory tierFactory : tierFactories) {
            tierConsumerAgents.add(
                    tierFactory.createConsumerAgent(
                            nettyConnectionReaders, remoteStorageScanner, remoteBufferSize));
        }
        return tierConsumerAgents;
    }
}
