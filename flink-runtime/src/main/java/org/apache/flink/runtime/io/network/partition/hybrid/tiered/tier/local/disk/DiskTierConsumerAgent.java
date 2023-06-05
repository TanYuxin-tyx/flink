package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.local.disk;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierConsumerAgent;

import java.io.IOException;
import java.util.Optional;

/** The data client is used to fetch data from disk tier. */
public class DiskTierConsumerAgent implements TierConsumerAgent {

    private final NettyConnectionReader[] nettyConnectionReaders;

    public DiskTierConsumerAgent(NettyConnectionReader[] nettyConnectionReaders) {
        this.nettyConnectionReaders = nettyConnectionReaders;
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
