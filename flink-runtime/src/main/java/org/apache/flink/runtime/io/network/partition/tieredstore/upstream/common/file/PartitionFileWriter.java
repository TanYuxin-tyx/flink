package org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.file;

import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.BufferContext;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * The {@link PartitionFileWriter} interface defines the write logic for different types of shuffle
 * files.
 */
public interface PartitionFileWriter {

    CompletableFuture<Void> spillAsync(List<BufferContext> bufferToSpill);

    void startSegment(int segmentIndex);

    void finishSegment(int segmentIndex);

    void release();
}
