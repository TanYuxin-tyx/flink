package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * The {@link PartitionFileWriter} interface defines the write logic for different types of shuffle
 * files.
 */
public interface PartitionFileWriter {

    /**
     * Write the {@link SubpartitionNettyPayload}s to the partition file. The written buffers may
     * belong to multiple subpartitions.
     *
     * @return the completable future indicating whether the writing file process has finished. If
     *     the {@link CompletableFuture} is completed, the written process is completed.
     */
    CompletableFuture<Void> write(List<SubpartitionNettyPayload> toWriteBuffers);

    /** Release all the resources of the {@link PartitionFileWriter}. */
    void release();
}
