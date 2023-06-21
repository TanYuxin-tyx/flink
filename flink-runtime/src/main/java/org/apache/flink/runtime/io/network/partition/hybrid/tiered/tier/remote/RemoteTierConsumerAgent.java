package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.remote;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierConsumerAgent;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** The data client is used to fetch data from DFS tier. */
public class RemoteTierConsumerAgent implements TierConsumerAgent {

    private final RemoteStorageFileScanner remoteStorageFileScanner;

    private final Map<TieredStoragePartitionId, Map<TieredStorageSubpartitionId, Integer>>
            requiredSegmentIds;

    RemoteTierConsumerAgent(RemoteStorageFileScanner remoteStorageFileScanner) {
        this.remoteStorageFileScanner = remoteStorageFileScanner;
        this.requiredSegmentIds = new HashMap<>();
    }

    @Override
    public void start() {
        remoteStorageFileScanner.start();
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
            remoteStorageFileScanner.registerSegmentId(partitionId, subpartitionId, segmentId);
            requiredSegmentIds.get(partitionId).put(subpartitionId, segmentId);
            return Optional.empty();
        }
        Buffer buffer = remoteStorageFileScanner.readBuffer(partitionId, subpartitionId, segmentId);
        return buffer == null ? Optional.empty() : Optional.of(buffer);
    }

    @Override
    public void close() throws IOException {
        remoteStorageFileScanner.close();
    }
}
