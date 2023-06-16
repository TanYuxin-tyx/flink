package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * The {@link PartitionFileWriter} interface defines the write logic for different types of shuffle
 * files.
 */
public interface PartitionFileWriter {

    CompletableFuture<Void> write(List<SubpartitionNettyPayload> toWriteBuffers);

    void release();
}
