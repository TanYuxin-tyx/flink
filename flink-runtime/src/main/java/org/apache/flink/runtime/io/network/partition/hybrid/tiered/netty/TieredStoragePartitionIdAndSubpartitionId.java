package org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty;

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;

/** The combination of {@link TieredStoragePartitionId} and {@link TieredStorageSubpartitionId}. */
public class TieredStoragePartitionIdAndSubpartitionId {

    private final TieredStoragePartitionId partitionId;

    private final TieredStorageSubpartitionId subpartitionId;

    private TieredStoragePartitionIdAndSubpartitionId(
            TieredStoragePartitionId partitionId, TieredStorageSubpartitionId subpartitionId) {
        this.partitionId = partitionId;
        this.subpartitionId = subpartitionId;
    }

    public static TieredStoragePartitionIdAndSubpartitionId create(
            TieredStoragePartitionId partitionId, TieredStorageSubpartitionId subpartitionId) {
        return new TieredStoragePartitionIdAndSubpartitionId(partitionId, subpartitionId);
    }

    @Override
    public String toString() {
        return "TieredStoragePartitionIdAndSubpartitionId{"
                + partitionId
                + " "
                + subpartitionId
                + '}';
    }
}
