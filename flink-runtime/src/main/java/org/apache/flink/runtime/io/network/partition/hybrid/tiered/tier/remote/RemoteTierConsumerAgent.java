package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.remote;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.PartitionFileReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierConsumerAgent;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.END_OF_SEGMENT;

/** The data client is used to fetch data from remote tier. */
public class RemoteTierConsumerAgent implements TierConsumerAgent {

    private final RemoteStorageFileScanner remoteStorageFileScanner;

    private final PartitionFileReader partitionFileReader;

    private final Map<TieredStoragePartitionId, Map<TieredStorageSubpartitionId, Integer>>
            requiredSegmentIds;

    RemoteTierConsumerAgent(
            RemoteStorageFileScanner remoteStorageFileScanner,
            PartitionFileReader partitionFileReader) {
        this.remoteStorageFileScanner = remoteStorageFileScanner;
        this.requiredSegmentIds = new HashMap<>();
        this.partitionFileReader = partitionFileReader;
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
        Buffer buffer = null;
        try {
            buffer =
                    partitionFileReader.readBuffer(
                            partitionId, subpartitionId, segmentId, -1, null, null);
        } catch (IOException e) {
            ExceptionUtils.rethrow(e, "Failed to read buffer from partition file.");
        }
        if (buffer != null && buffer.getDataType() != END_OF_SEGMENT) {
            remoteStorageFileScanner.triggerSubpartitionReading(partitionId, subpartitionId);
        }
        return buffer == null ? Optional.empty() : Optional.of(buffer);
    }

    @Override
    public void close() throws IOException {
        remoteStorageFileScanner.close();
    }
}
