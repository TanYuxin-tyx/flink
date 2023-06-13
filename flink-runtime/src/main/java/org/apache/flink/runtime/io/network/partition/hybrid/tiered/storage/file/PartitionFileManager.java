package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file;

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.ioscheduler.DiskIOScheduler;

import java.util.List;
import java.util.Map;

/**
 * The {@link PartitionFileManager} interface can create writers and readers for different types of
 * shuffle files.
 */
public interface PartitionFileManager {

    PartitionFileWriter createPartitionFileWriter(PartitionFileType partitionFileType);

    DiskIOScheduler createDiskIOScheduler(
            TieredStorageNettyService nettyService,
            List<Map<Integer, Integer>> firstBufferContextInSegment);
}
