package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierConsumerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.disk.DiskTierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.memory.MemoryTierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.remote.RemoteTierFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/** {@link TieredStorageConsumerClient} is used to read buffer from tiered store. */
public class TieredStorageConsumerClient {

    private final List<TierFactory> tierFactories;

    private final List<TierConsumerAgent> tierConsumerAgents;

    private final int[] subpartitionNextSegmentIds;

    public TieredStorageConsumerClient(
            List<Tuple2<TieredStoragePartitionId, TieredStorageSubpartitionId>>
                    partitionIdAndSubpartitionIds,
            TieredStorageNettyService nettyService,
            JobID jobID,
            NetworkBufferPool networkBufferPool,
            String baseRemoteStoragePath,
            boolean isUpstreamBroadcast,
            BiConsumer<Integer, Boolean> queueChannelCallBack) {
        this.tierFactories = createTierFactories(baseRemoteStoragePath);
        this.tierConsumerAgents =
                createTierConsumerAgents(
                        partitionIdAndSubpartitionIds,
                        jobID,
                        networkBufferPool,
                        baseRemoteStoragePath,
                        nettyService,
                        isUpstreamBroadcast,
                        queueChannelCallBack);
        this.subpartitionNextSegmentIds = new int[partitionIdAndSubpartitionIds.size()];
    }

    public void start() {
        for (TierConsumerAgent tierConsumerAgent : tierConsumerAgents) {
            tierConsumerAgent.start();
        }
    }

    public Optional<Buffer> getNextBuffer(int subpartitionId) {
        Optional<Buffer> bufferAndAvailability = Optional.empty();
        for (TierConsumerAgent tiereConsumerAgent : tierConsumerAgents) {
            bufferAndAvailability =
                    tiereConsumerAgent.getNextBuffer(
                            subpartitionId, subpartitionNextSegmentIds[subpartitionId]);
            if (bufferAndAvailability.isPresent()) {
                break;
            }
        }
        if (!bufferAndAvailability.isPresent()) {
            return Optional.empty();
        }
        Buffer bufferData = bufferAndAvailability.get();
        if (bufferData.getDataType() == Buffer.DataType.END_OF_SEGMENT) {
            subpartitionNextSegmentIds[subpartitionId]++;
            bufferData.recycleBuffer();
            return getNextBuffer(subpartitionId);
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
            tierFactories.add(new RemoteTierFactory(0));
        }
        return tierFactories;
    }

    private List<TierConsumerAgent> createTierConsumerAgents(
            List<Tuple2<TieredStoragePartitionId, TieredStorageSubpartitionId>>
                    partitionIdAndSubpartitionIds,
            JobID jobID,
            NetworkBufferPool networkBufferPool,
            String baseRemoteStoragePath,
            TieredStorageNettyService nettyService,
            boolean isUpstreamBroadcastOnly,
            BiConsumer<Integer, Boolean> queueChannelCallBack) {
        List<TierConsumerAgent> tierConsumerAgents = new ArrayList<>();
        List<CompletableFuture<NettyConnectionReader>> nettyConnectionReaders = new ArrayList<>();
        for (int index = 0; index < partitionIdAndSubpartitionIds.size(); ++index) {
            nettyConnectionReaders.add(
                    nettyService.registerConsumer(
                            partitionIdAndSubpartitionIds.get(index).f0,
                            partitionIdAndSubpartitionIds.get(index).f1));
        }
        for (TierFactory tierFactory : tierFactories) {
            tierConsumerAgents.add(
                    tierFactory.createConsumerAgent(
                            partitionIdAndSubpartitionIds,
                            jobID,
                            networkBufferPool,
                            baseRemoteStoragePath,
                            nettyService,
                            isUpstreamBroadcastOnly,
                            queueChannelCallBack,
                            nettyConnectionReaders));
        }
        return tierConsumerAgents;
    }
}
