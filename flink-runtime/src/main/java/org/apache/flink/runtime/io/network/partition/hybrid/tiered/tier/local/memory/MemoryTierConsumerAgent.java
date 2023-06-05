package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.local.memory;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStoragePartitionIdAndSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierConsumerAgent;

import java.io.IOException;
import java.util.Optional;

/** The data client is used to fetch data from memory tier. */
public class MemoryTierConsumerAgent implements TierConsumerAgent {
    private final NettyConnectionReader[] nettyConnectionReaders;

    public MemoryTierConsumerAgent(int numSubpartitions, TieredStoragePartitionIdAndSubpartitionId[] ids, TieredStorageNettyService nettyService) {
        this.nettyConnectionReaders = new NettyConnectionReader[numSubpartitions];
        for(int index = 0; index < numSubpartitions; ++index){
            nettyConnectionReaders[index] = nettyService.registerConsumer(ids[index]);
        }
    }

    @Override
    public void start() {
        // nothing to do.
    }

    @Override
    public Optional<Buffer> getNextBuffer(int subpartitionId, int segmentId) {
        return nettyConnectionReaders[subpartitionId].readBuffer(segmentId);
    }

    @Override
    public void close() throws IOException {
        // nothing to do.
    }
}
