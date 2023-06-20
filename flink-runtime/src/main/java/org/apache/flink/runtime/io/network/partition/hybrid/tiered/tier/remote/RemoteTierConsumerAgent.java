package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.remote;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.HashPartitionFileReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.PartitionFileReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierConsumerAgent;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.END_OF_SEGMENT;

/** The data client is used to fetch data from DFS tier. */
public class RemoteTierConsumerAgent implements TierConsumerAgent {

    private final RemoteTierMonitor remoteTierMonitor;

    private final BiConsumer<Integer, Boolean> queueChannelCallBack;

    private final Map<TieredStoragePartitionId, Map<TieredStorageSubpartitionId, Integer>>
            requiredSegmentIds;

    private final Map<TieredStoragePartitionId, Map<TieredStorageSubpartitionId, Integer>>
            channelIndexes;

    private final PartitionFileReader partitionFileReader;

    public RemoteTierConsumerAgent(
            String baseRemoteStoragePath,
            JobID jobID,
            Boolean isUpStreamBroadCastOnly,
            List<Tuple2<TieredStoragePartitionId, TieredStorageSubpartitionId>>
                    partitionIdAndSubpartitionIds,
            RemoteTierMonitor remoteTierMonitor,
            BiConsumer<Integer, Boolean> queueChannelCallBack) {
        this.remoteTierMonitor = remoteTierMonitor;
        this.queueChannelCallBack = queueChannelCallBack;
        this.requiredSegmentIds = new HashMap<>();
        this.channelIndexes = new HashMap<>();
        this.partitionFileReader =
                new HashPartitionFileReader(baseRemoteStoragePath, jobID, isUpStreamBroadCastOnly);
        for (int index = 0; index < partitionIdAndSubpartitionIds.size(); ++index) {
            Tuple2<TieredStoragePartitionId, TieredStorageSubpartitionId> ids =
                    partitionIdAndSubpartitionIds.get(index);
            channelIndexes.computeIfAbsent(ids.f0, ignore -> new HashMap<>()).put(ids.f1, index);
        }
    }

    @Override
    public void start() {
        remoteTierMonitor.start();
    }

    @Override
    public Optional<Buffer> getNextBuffer(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId) {
        if (segmentId
                != requiredSegmentIds
                        .computeIfAbsent(partitionId, ignore -> new HashMap<>())
                        .getOrDefault(subpartitionId, -1)) {
            remoteTierMonitor.monitorSegmentFile(partitionId, subpartitionId, segmentId);
            requiredSegmentIds.get(partitionId).put(subpartitionId, segmentId);
            return Optional.empty();
        }
        Buffer buffer = null;
        try {
            buffer =
                    partitionFileReader.readBuffer(
                            partitionId, subpartitionId, segmentId, -1, null, null);
        } catch (IOException e) {
            ExceptionUtils.rethrow(e, "Failed to read buffer from partition file.");
        }
        if (buffer != null && buffer.getDataType() != END_OF_SEGMENT) {
            queueChannelCallBack.accept(channelIndexes.get(partitionId).get(subpartitionId), false);
        }
        return buffer == null ? Optional.empty() : Optional.of(buffer);
    }

    @Override
    public void close() throws IOException {
        remoteTierMonitor.close();
    }
}
