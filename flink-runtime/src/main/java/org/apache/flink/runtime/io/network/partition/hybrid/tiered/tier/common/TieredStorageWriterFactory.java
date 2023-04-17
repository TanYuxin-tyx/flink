package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.common;

import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.shuffle.TierType;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.remote.RemoteCacheManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.remote.RemoteTierProducerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.common.CacheFlushManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.common.file.PartitionFileManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.common.file.PartitionFileType;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.local.disk.DiskCacheManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.local.disk.DiskTierProducerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.local.memory.MemoryTierProducerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.local.memory.SubpartitionMemoryDataManager;

import java.io.IOException;

/** {@link TieredStorageWriterFactory} is used to create writer of each tier. */
public class TieredStorageWriterFactory {

    private final boolean isBroadcastOnly;

    private final int numSubpartitions;

    private final int networkBufferSize;

    private final TieredStoreMemoryManager tieredStoreMemoryManager;

    private final BufferCompressor bufferCompressor;

    private final CacheFlushManager cacheFlushManager;

    private final PartitionFileManager partitionFileManager;

    public TieredStorageWriterFactory(
            boolean isBroadcastOnly,
            int numSubpartitions,
            int networkBufferSize,
            TieredStoreMemoryManager tieredStoreMemoryManager,
            BufferCompressor bufferCompressor,
            CacheFlushManager cacheFlushManager,
            PartitionFileManager partitionFileManager) {

        this.isBroadcastOnly = isBroadcastOnly;
        this.numSubpartitions = numSubpartitions;
        this.networkBufferSize = networkBufferSize;
        this.tieredStoreMemoryManager = tieredStoreMemoryManager;
        this.bufferCompressor = bufferCompressor;
        this.cacheFlushManager = cacheFlushManager;
        this.partitionFileManager = partitionFileManager;
    }

    public TierProducerAgent createTierStorageWriter(TierType tierType) throws IOException {
        TierProducerAgent tierProducerAgent;
        switch (tierType) {
            case IN_MEM:
                tierProducerAgent = getMemoryTierStorageWriter();
                break;
            case IN_DISK:
                tierProducerAgent = getDiskTierStorageWriter();
                break;
            case IN_REMOTE:
                tierProducerAgent = getRemoteTierStorageWriter();
                break;
            default:
                throw new IllegalArgumentException("Illegal tier type " + tierType);
        }
        return tierProducerAgent;
    }

    private TierProducerAgent getMemoryTierStorageWriter() {
        return new MemoryTierProducerAgent(
                isBroadcastOnly ? 1 : numSubpartitions,
                networkBufferSize,
                tieredStoreMemoryManager,
                bufferCompressor,
                new SubpartitionSegmentIndexTrackerImpl(numSubpartitions, isBroadcastOnly),
                isBroadcastOnly,
                numSubpartitions,
                new SubpartitionMemoryDataManager[numSubpartitions]);
    }

    private TierProducerAgent getRemoteTierStorageWriter() {
        return new RemoteTierProducerAgent(
                numSubpartitions,
                new SubpartitionSegmentIndexTrackerImpl(numSubpartitions, isBroadcastOnly),
                new RemoteCacheManager(
                        isBroadcastOnly ? 1 : numSubpartitions,
                        networkBufferSize,
                        tieredStoreMemoryManager,
                        cacheFlushManager,
                        bufferCompressor,
                        partitionFileManager.createPartitionFileWriter(
                                PartitionFileType.PRODUCER_HASH)));
    }

    private TierProducerAgent getDiskTierStorageWriter() {
        return new DiskTierProducerAgent(
                new int[numSubpartitions],
                new SubpartitionSegmentIndexTrackerImpl(numSubpartitions, isBroadcastOnly),
                new DiskCacheManager(
                        isBroadcastOnly ? 1 : numSubpartitions,
                        networkBufferSize,
                        tieredStoreMemoryManager,
                        cacheFlushManager,
                        bufferCompressor,
                        partitionFileManager));
    }
}
