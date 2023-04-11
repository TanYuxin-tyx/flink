package org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.file;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.TieredStoreConfiguration;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.tier.local.disk.RegionBufferIndexTracker;

import java.nio.file.Path;
import java.util.concurrent.ScheduledExecutorService;

/** THe implementation of {@link PartitionFileManager}. */
public class PartitionFileManagerImpl implements PartitionFileManager {

    // For Writer PRODUCER_MERGE

    private final Path producerMergeShuffleFilePath;

    private final RegionBufferIndexTracker producerMergeIndex;

    // For Reader PRODUCER_MERGE

    private final BatchShuffleReadBufferPool readBufferPool;

    private final ScheduledExecutorService readIOExecutor;

    private final TieredStoreConfiguration storeConfiguration;

    // For Writer PRODUCER_HASH

    private final int numSubpartitions;

    private final JobID jobID;

    private final ResultPartitionID resultPartitionID;

    private final String hashShuffleDataPath;

    public PartitionFileManagerImpl(
            Path producerMergeShuffleFilePath,
            RegionBufferIndexTracker producerMergeIndex,
            BatchShuffleReadBufferPool readBufferPool,
            ScheduledExecutorService readIOExecutor,
            TieredStoreConfiguration storeConfiguration,
            int numSubpartitions,
            JobID jobID,
            ResultPartitionID resultPartitionID,
            String hashShuffleFilePath) {
        this.producerMergeShuffleFilePath = producerMergeShuffleFilePath;
        this.producerMergeIndex = producerMergeIndex;
        this.readBufferPool = readBufferPool;
        this.readIOExecutor = readIOExecutor;
        this.storeConfiguration = storeConfiguration;
        this.numSubpartitions = numSubpartitions;
        this.jobID = jobID;
        this.resultPartitionID = resultPartitionID;
        this.hashShuffleDataPath = hashShuffleFilePath;
    }

    @Override
    public PartitionFileWriter createPartitionFileWriter(PartitionFileType partitionFileType) {
        switch (partitionFileType) {
            case PRODUCER_MERGE:
                return new ProducerMergePartitionFileWriter(
                        producerMergeShuffleFilePath, producerMergeIndex);
            case PRODUCER_HASH:
                return new HashPartitionFileWriter(
                        jobID, numSubpartitions, resultPartitionID, hashShuffleDataPath);
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
                        producerMergeIndex,
                        producerMergeShuffleFilePath,
                        ProducerMergePartitionTierConsumerImpl.Factory.INSTANCE,
                        storeConfiguration);
        }
        throw new UnsupportedOperationException(
                "PartitionFileManager doesn't support the type of partition file: "
                        + partitionFileType);
    }
}