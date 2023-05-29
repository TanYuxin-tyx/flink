package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file;

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty2.ProducerNettyService;

/**
 * The {@link PartitionFileManager} interface can create writers and readers for different types of
 * shuffle files.
 */
public interface PartitionFileManager {

    PartitionFileWriter createPartitionFileWriter(PartitionFileType partitionFileType);

    PartitionFileReader createPartitionFileReader(PartitionFileType partitionFileTyp, ProducerNettyService nettyService);
}
