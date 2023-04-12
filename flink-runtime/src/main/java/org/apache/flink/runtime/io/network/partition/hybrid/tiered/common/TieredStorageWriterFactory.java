package org.apache.flink.runtime.io.network.partition.hybrid.tiered.common;

import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.TierType;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.remote.RemoteCacheManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.remote.RemoteTierStorageWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.CacheFlushManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.file.PartitionFileManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.file.PartitionFileType;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.local.disk.DiskCacheManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.local.disk.DiskTierStorageWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.local.memory.MemoryTierStorageWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.local.memory.SubpartitionMemoryDataManager;

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

    public TierStorageWriter createTierStorageWriter(TierType tierType) throws IOException {
        TierStorageWriter tierStorageWriter;
        switch (tierType) {
            case IN_MEM:
                tierStorageWriter = getMemoryTierStorageWriter();
                break;
            case IN_DISK:
                tierStorageWriter = getDiskTierStorageWriter();
                break;
            case IN_REMOTE:
                tierStorageWriter = getRemoteTierStorageWriter();
                break;
            default:
                throw new IllegalArgumentException("Illegal tier type " + tierType);
        }
        return tierStorageWriter;
    }

    private TierStorageWriter getMemoryTierStorageWriter() {
        return new MemoryTierStorageWriter(
                isBroadcastOnly ? 1 : numSubpartitions,
                networkBufferSize,
                tieredStoreMemoryManager,
                bufferCompressor,
                new SubpartitionSegmentIndexTrackerImpl(numSubpartitions, isBroadcastOnly),
                isBroadcastOnly,
                numSubpartitions,
                new SubpartitionMemoryDataManager[numSubpartitions]);
    }

    private TierStorageWriter getRemoteTierStorageWriter() {
        return new RemoteTierStorageWriter(
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

    private TierStorageWriter getDiskTierStorageWriter() {
        return new DiskTierStorageWriter(
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
