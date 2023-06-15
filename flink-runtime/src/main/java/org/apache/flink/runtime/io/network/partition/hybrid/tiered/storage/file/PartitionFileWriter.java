package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file;

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyPayload;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * The {@link PartitionFileWriter} interface defines the write logic for different types of shuffle
 * files.
 */
public interface PartitionFileWriter {

    CompletableFuture<Void> spillAsync(
            int subpartitionId, int segmentId, List<NettyPayload> bufferToSpill);

    CompletableFuture<Void> finishSegment(int subpartitionId, int segmentId);

    void release();
}
