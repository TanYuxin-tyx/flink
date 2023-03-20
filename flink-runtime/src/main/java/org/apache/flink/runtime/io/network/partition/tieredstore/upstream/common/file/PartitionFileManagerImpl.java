package org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.file;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.TieredStoreConfiguration;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.DiskTierReaderImpl;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.RegionBufferIndexTracker;

import java.nio.file.Path;
import java.util.concurrent.ScheduledExecutorService;

/** THe implementation of {@link PartitionFileManager}. */
public class PartitionFileManagerImpl implements PartitionFileManager {

    // For Writer PRODUCER_MERGE

    private final Path dataFilePath;

    private final RegionBufferIndexTracker regionBufferIndexTracker;

    // For Reader PRODUCER_MERGE

    private final BatchShuffleReadBufferPool readBufferPool;

    private final ScheduledExecutorService readIOExecutor;

    private final TieredStoreConfiguration storeConfiguration;

    // For Writer PRODUCER_HASH

    private final int numSubpartitions;

    private JobID jobID;

    private ResultPartitionID resultPartitionID;

    private String baseShuffleDataPath;

    public PartitionFileManagerImpl(
            Path dataFilePath,
            RegionBufferIndexTracker regionBufferIndexTracker,
            BatchShuffleReadBufferPool readBufferPool,
            ScheduledExecutorService readIOExecutor,
            TieredStoreConfiguration storeConfiguration,
            int numSubpartitions,
            JobID jobID,
            ResultPartitionID resultPartitionID,
            String baseShuffleDataPath) {
        this.dataFilePath = dataFilePath;
        this.regionBufferIndexTracker = regionBufferIndexTracker;
        this.readBufferPool = readBufferPool;
        this.readIOExecutor = readIOExecutor;
        this.storeConfiguration = storeConfiguration;
        this.numSubpartitions = numSubpartitions;
        this.jobID = jobID;
        this.resultPartitionID = resultPartitionID;
        this.baseShuffleDataPath = baseShuffleDataPath;
    }

    @Override
    public PartitionFileWriter createPartitionFileWriter(PartitionFileType partitionFileType) {
        switch (partitionFileType) {
            case PRODUCER_MERGE:
                return new ProducerMergePartitionFileWriter(dataFilePath, regionBufferIndexTracker);
            case PRODUCER_HASH:
                return new HashPartitionFileWriter(
                        jobID, numSubpartitions, resultPartitionID, baseShuffleDataPath);
        }
        throw new UnsupportedOperationException(
                "PartitionFileManager doesn't support the type of partition file: "
                        + partitionFileType);
    }

    @Override
    public PartitionFileReader createPartitionFileReader(PartitionFileType partitionFileType) {
        switch (partitionFileType) {
            case PRODUCER_MERGE:
                return new ProducerMergePartitionFileReader(
                        readBufferPool,
                        readIOExecutor,
                        regionBufferIndexTracker,
                        dataFilePath,
                        DiskTierReaderImpl.Factory.INSTANCE,
                        storeConfiguration);
        }
        throw new UnsupportedOperationException(
                "PartitionFileManager doesn't support the type of partition file: "
                        + partitionFileType);
    }
}
