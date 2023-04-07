package org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.file;

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.BufferContext;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * The {@link PartitionFileWriter} interface defines the write logic for different types of shuffle
 * files.
 */
public interface PartitionFileWriter {

    CompletableFuture<Void> spillAsync(List<BufferContext> bufferToSpill);

    CompletableFuture<Void> spillAsync(
            int subpartitionId, int segmentId, List<BufferContext> bufferToSpill);

    CompletableFuture<Void> finishSegment(int subpartitionId, int segmentId);

    void release();
}
