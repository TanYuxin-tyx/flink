package org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.file;

/**
 * The {@link PartitionFileManager} interface can create writers and readers for different types of
 * shuffle files.
 */
public interface PartitionFileManager {

    PartitionFileWriter createPartitionFileWriter(PartitionFileType partitionFileType);

    PartitionFileReader createPartitionFileReader(PartitionFileType partitionFileType);
}