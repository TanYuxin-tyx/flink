package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file;

/**
 * The {@link PartitionFileType} interface defines different types of Shuffle files. These files
 * have their own read and write logic.
 */
public enum PartitionFileType {
    PRODUCER_MERGE,
    PRODUCER_HASH,
    CONSUMER_MERGE // Not Supported
}
