package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.common.file;

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.service.NettyBufferQueue;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.service.NettyServiceView;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.service.NettyServiceViewId;

import java.io.IOException;

/**
 * The {@link PartitionFileReader} interface defines the read logic for different types of shuffle
 * files.
 */
public interface PartitionFileReader {

    int read();

    NettyBufferQueue createNettyBufferQueue(
            int subpartitionId,
            NettyServiceViewId nettyServiceViewId,
            NettyServiceView tierConsumerView)
            throws IOException;

    void release();
}
