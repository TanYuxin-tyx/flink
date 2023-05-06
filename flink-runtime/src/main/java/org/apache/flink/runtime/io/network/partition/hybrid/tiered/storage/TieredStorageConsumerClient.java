package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
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

/** {@link TieredStorageConsumerClient} is used to read buffer from tiered store. */
public class TieredStorageConsumerClient {

    private final List<TierFactory> tierFactories;

    private final List<TierConsumerAgent> tierConsumerAgents;

    private final int[] latestSegmentIds;

    private final NettyService consumerNettyService;

    public TieredStorageConsumerClient(
            int numSubpartitions,
            List<Integer> subpartitionIds,
            JobID jobID,
            List<ResultPartitionID> resultPartitionIDs,
            NetworkBufferPool networkBufferPool,
            String baseRemoteStoragePath,
            NettyService consumerNettyService,
            boolean isUpstreamBroadcast) {
        this.tierFactories = createTierFactories(baseRemoteStoragePath);
        this.tierConsumerAgents =
                createTierConsumerAgents(
                        numSubpartitions,
                        subpartitionIds,
                        jobID,
                        resultPartitionIDs,
                        networkBufferPool,
                        baseRemoteStoragePath,
                        consumerNettyService,
                        isUpstreamBroadcast);
        this.latestSegmentIds = new int[numSubpartitions];
        this.consumerNettyService = consumerNettyService;
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
                            subpartitionId, latestSegmentIds[subpartitionId]);
            if (bufferAndAvailability.isPresent()) {
                break;
            }
        }
        if (!bufferAndAvailability.isPresent()) {
            return Optional.empty();
        }
        Buffer bufferData = bufferAndAvailability.get();
        if (bufferData.getDataType() == Buffer.DataType.ADD_SEGMENT_ID_EVENT) {
            latestSegmentIds[subpartitionId]++;
            bufferData.recycleBuffer();
            consumerNettyService.notifyResultSubpartitionAvailable(subpartitionId, false);
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
        tierFactories.add(new MemoryTierFactory());
        tierFactories.add(new DiskTierFactory());
        if (baseRemoteStoragePath != null) {
            tierFactories.add(new RemoteTierFactory());
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
            NettyService consumerNettyService,
            boolean isUpstreamBroadcastOnly) {
        Set<TierConsumerAgent> tierConsumerAgents = new HashSet<>();
        for (TierFactory tierFactory : tierFactories) {
            tierConsumerAgents.add(
                    tierFactory.createConsumerAgent(
                            numSubpartitions,
                            subpartitionIds,
                            jobID,
                            resultPartitionIDs,
                            networkBufferPool,
                            baseRemoteStoragePath,
                            consumerNettyService,
                            isUpstreamBroadcastOnly));
        }
        return new ArrayList<>(tierConsumerAgents);
    }
}
