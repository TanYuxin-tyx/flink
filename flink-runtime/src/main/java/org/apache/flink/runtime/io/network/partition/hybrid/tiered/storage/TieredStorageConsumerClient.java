package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.network.buffer.Buffer;
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
     * This map is used to record the consumer agent being used and the id of segment being read for
     * each data source, which is represented by {@link TieredStoragePartitionId} and {@link
     * TieredStorageSubpartitionId}.
     */
    private final Map<
                    TieredStoragePartitionId,
                    Map<TieredStorageSubpartitionId, Tuple2<TierConsumerAgent, Integer>>>
            currentConsumerAgentAndSegmentIds = new HashMap<>();

    public TieredStorageConsumerClient(
            List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs,
            TieredStorageNettyService nettyService,
            String baseRemoteStoragePath,
            RemoteStorageScanner remoteStorageScanner,
            PartitionFileReader partitionFileReader,
            int remoteBufferSize) {
        this.tierFactories = createTierFactories(baseRemoteStoragePath);
        this.tierConsumerAgents =
                createTierConsumerAgents(
                        tieredStorageConsumerSpecs,
                        nettyService,
                        remoteStorageScanner,
                        partitionFileReader, remoteBufferSize);
    }

    public void start() {
        for (TierConsumerAgent tierConsumerAgent : tierConsumerAgents) {
            tierConsumerAgent.start();
        }
    }

    public Optional<Buffer> getNextBuffer(
            TieredStoragePartitionId partitionId, TieredStorageSubpartitionId subpartitionId) {
        Tuple2<TierConsumerAgent, Integer> currentConsumerAgentAndSegmentId =
                currentConsumerAgentAndSegmentIds
                        .computeIfAbsent(partitionId, ignore -> new HashMap<>())
                        .getOrDefault(subpartitionId, Tuple2.of(null, 0));
        Optional<Buffer> buffer = Optional.empty();
        if (currentConsumerAgentAndSegmentId.f0 == null) {
            for (TierConsumerAgent tierConsumerAgent : tierConsumerAgents) {
                buffer =
                        tierConsumerAgent.getNextBuffer(
                                partitionId, subpartitionId, currentConsumerAgentAndSegmentId.f1);
                if (buffer.isPresent()) {
                    currentConsumerAgentAndSegmentIds
                            .get(partitionId)
                            .put(
                                    subpartitionId,
                                    Tuple2.of(
                                            tierConsumerAgent,
                                            currentConsumerAgentAndSegmentId.f1));
                    break;
                }
            }
        } else {
            buffer =
                    currentConsumerAgentAndSegmentId.f0.getNextBuffer(
                            partitionId, subpartitionId, currentConsumerAgentAndSegmentId.f1);
        }
        if (!buffer.isPresent()) {
            return Optional.empty();
        }
        Buffer bufferData = buffer.get();
        if (bufferData.getDataType() == Buffer.DataType.END_OF_SEGMENT) {
            currentConsumerAgentAndSegmentIds
                    .get(partitionId)
                    .put(subpartitionId, Tuple2.of(null, currentConsumerAgentAndSegmentId.f1 + 1));
            bufferData.recycleBuffer();
            return getNextBuffer(partitionId, subpartitionId);
        }
        return Optional.of(bufferData);
    }

    public void close() throws IOException {
        for (TierConsumerAgent tierConsumerAgent : tierConsumerAgents) {
            tierConsumerAgent.close();
        }
    }

    private List<TierFactory> createTierFactories(String baseRemoteStoragePath) {
        List<TierFactory> tierFactories = new ArrayList<>();
        tierFactories.add(new MemoryTierFactory(0, 0));
        tierFactories.add(new DiskTierFactory(0, 0, 0));
        if (baseRemoteStoragePath != null) {
            tierFactories.add(new RemoteTierFactory(0, 0));
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
                            nettyConnectionReaders,
                            remoteStorageScanner,
                            partitionFileReader,
                            remoteBufferSize));
        }
        return tierConsumerAgents;
    }
}
