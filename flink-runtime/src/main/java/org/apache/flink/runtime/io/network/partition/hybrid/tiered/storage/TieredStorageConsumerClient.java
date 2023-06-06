package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStoragePartitionIdAndSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierConsumerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.local.disk.DiskTierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.local.memory.MemoryTierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.remote.RemoteTierFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;

/** {@link TieredStorageConsumerClient} is used to read buffer from tiered store. */
public class TieredStorageConsumerClient {

    private final List<TierFactory> tierFactories;

    private final List<TierConsumerAgent> tierConsumerAgents;

    private final int[] subpartitionNextSegmentIds;

    public TieredStorageConsumerClient(
            int numSubpartitions,
            List<Integer> subpartitionIds,
            JobID jobID,
            List<ResultPartitionID> resultPartitionIDs,
            NetworkBufferPool networkBufferPool,
            String baseRemoteStoragePath,
            TieredStorageNettyService nettyService,
            TieredStoragePartitionIdAndSubpartitionId[] ids,
            boolean isUpstreamBroadcast,
            BiConsumer<Integer, Boolean> queueChannelCallBack) {
        this.tierFactories = createTierFactories(baseRemoteStoragePath);
        this.tierConsumerAgents =
                createTierConsumerAgents(
                        numSubpartitions,
                        subpartitionIds,
                        jobID,
                        resultPartitionIDs,
                        networkBufferPool,
                        baseRemoteStoragePath,
                        ids,
                        nettyService,
                        isUpstreamBroadcast,
                        queueChannelCallBack);
        this.subpartitionNextSegmentIds = new int[numSubpartitions];
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
        tierFactories.add(new MemoryTierFactory(0));
        tierFactories.add(new DiskTierFactory(0));
        if (baseRemoteStoragePath != null) {
            tierFactories.add(new RemoteTierFactory(0));
        }
        return tierFactories;
    }

    private List<TierConsumerAgent> createTierConsumerAgents(
            int numSubpartitions,
            List<Integer> subpartitionIds,
            JobID jobID,
            List<ResultPartitionID> resultPartitionIDs,
            NetworkBufferPool networkBufferPool,
            String baseRemoteStoragePath,
            TieredStoragePartitionIdAndSubpartitionId[] ids,
            TieredStorageNettyService nettyService,
            boolean isUpstreamBroadcastOnly,
            BiConsumer<Integer, Boolean> queueChannelCallBack) {
        List<TierConsumerAgent> tierConsumerAgents = new ArrayList<>();
        NettyConnectionReader[] nettyConnectionReaders =
                new NettyConnectionReader[numSubpartitions];
        for (int index = 0; index < numSubpartitions; ++index) {
            nettyConnectionReaders[index] = nettyService.registerConsumer(ids[index]);
        }
        for (TierFactory tierFactory : tierFactories) {
            tierConsumerAgents.add(
                    tierFactory.createConsumerAgent(
                            numSubpartitions,
                            subpartitionIds,
                            jobID,
                            resultPartitionIDs,
                            networkBufferPool,
                            baseRemoteStoragePath,
                            ids,
                            nettyService,
                            isUpstreamBroadcastOnly,
                            queueChannelCallBack,
                            nettyConnectionReaders));
        }
        return tierConsumerAgents;
    }
}
