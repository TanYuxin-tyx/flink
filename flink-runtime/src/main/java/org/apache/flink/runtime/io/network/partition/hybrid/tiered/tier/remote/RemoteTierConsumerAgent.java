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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.END_OF_SEGMENT;

/** The data client is used to fetch data from DFS tier. */
public class RemoteTierConsumerAgent implements TierConsumerAgent {

    private final int[] requiredSegmentIds;

    private final RemoteTierMonitor remoteTierMonitor;

    private final BiConsumer<Integer, Boolean> queueChannelCallBack;

    private final Map<TieredStoragePartitionId, Map<TieredStorageSubpartitionId, Integer>>
            subpartitionIndexs = new HashMap<>();

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
        this.requiredSegmentIds = new int[partitionIdAndSubpartitionIds.size()];
        Arrays.fill(requiredSegmentIds, -1);
        for (int index = 0; index < partitionIdAndSubpartitionIds.size(); ++index) {
            Tuple2<TieredStoragePartitionId, TieredStorageSubpartitionId> ids =
                    partitionIdAndSubpartitionIds.get(index);
            subpartitionIndexs
                    .computeIfAbsent(ids.f0, ignore -> new HashMap<>())
                    .put(ids.f1, index);
        }
        this.partitionFileReader =
                new HashPartitionFileReader(baseRemoteStoragePath, jobID, isUpStreamBroadCastOnly);
    }

    @Override
    public void start() {
        remoteTierMonitor.start();
    }

    @Override
    public Optional<Buffer> getNextBuffer(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId2,
            int segmentId) {
        int subpartitionId = subpartitionIndexs.get(partitionId).get(subpartitionId2);
        if (segmentId != requiredSegmentIds[subpartitionId]) {
            remoteTierMonitor.monitorSegmentFile(subpartitionId, segmentId);
            requiredSegmentIds[subpartitionId] = segmentId;
            return Optional.empty();
        }
        Buffer buffer = null;
        try {
            buffer =
                    partitionFileReader.readBuffer(
                            partitionId, subpartitionId2, segmentId, -1, null, null);
        } catch (IOException e) {
            ExceptionUtils.rethrow(e, "Failed to read buffer from partition file.");
        }
        if (buffer != null && buffer.getDataType() != END_OF_SEGMENT) {
            queueChannelCallBack.accept(subpartitionId, false);
        }
        return buffer == null ? Optional.empty() : Optional.of(buffer);
    }

    @Override
    public void close() throws IOException {
        remoteTierMonitor.close();
    }
}
