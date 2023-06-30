package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.remote;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
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

    private final RemoteStorageScanner remoteStorageScanner;

    private final PartitionFileReader partitionFileReader;

    /**
     * The current reading buffer indexes and segment ids stored in map.
     *
     * <p>The key is partition id and subpartition id. The value is buffer index and segment id.
     */
    private final Map<
                    TieredStoragePartitionId,
                    Map<TieredStorageSubpartitionId, Tuple2<Integer, Integer>>>
            currentBufferIndexAndSegmentIds;

    private final int remoteBufferSize;

    RemoteTierConsumerAgent(
            RemoteStorageScanner remoteStorageScanner,
            PartitionFileReader partitionFileReader,
            int remoteBufferSize) {
        this.remoteStorageScanner = remoteStorageScanner;
        this.currentBufferIndexAndSegmentIds = new HashMap<>();
        this.partitionFileReader = partitionFileReader;
        this.remoteBufferSize = remoteBufferSize;
    }

    @Override
    public void start() {
        remoteStorageScanner.start();
    }

    @Override
    public Optional<Buffer> getNextBuffer(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId) {
        Tuple2<Integer, Integer> bufferIndexAndSegmentId =
                currentBufferIndexAndSegmentIds
                        .computeIfAbsent(partitionId, ignore -> new HashMap<>())
                        .getOrDefault(subpartitionId, Tuple2.of(0, -1));
        int currentBufferIndex = bufferIndexAndSegmentId.f0;
        int currentSegmentId = bufferIndexAndSegmentId.f1;
        if (segmentId != currentSegmentId) {
            remoteStorageScanner.registerSegmentId(partitionId, subpartitionId, segmentId);
        }
        MemorySegment memorySegment =
                MemorySegmentFactory.allocateUnpooledSegment(remoteBufferSize);
        Buffer buffer = null;
        try {
            buffer =
                    partitionFileReader.readBuffer(
                            partitionId,
                            subpartitionId,
                            segmentId,
                            currentBufferIndex,
                            memorySegment,
                            FreeingBufferRecycler.INSTANCE);
        } catch (IOException e) {
            ExceptionUtils.rethrow(e, "Failed to read buffer from partition file.");
        }
        if (buffer != null && buffer.getDataType() != END_OF_SEGMENT) {
            remoteStorageScanner.triggerNextRoundReading(
                    partitionId, subpartitionId, buffer.getDataType().hasPriority());
        }
        if (buffer != null) {
            if (buffer.getDataType().hasPriority()) {
                remoteStorageScanner.updatePrioritySequenceNumber(
                        partitionId, subpartitionId, currentBufferIndex);
            }
            currentBufferIndexAndSegmentIds
                    .get(partitionId)
                    .put(subpartitionId, Tuple2.of(++currentBufferIndex, segmentId));
            return Optional.of(buffer);
        }
        return Optional.empty();
    }

    @Override
    public void close() throws IOException {
        remoteStorageScanner.close();
    }
}
