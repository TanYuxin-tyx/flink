package org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.file;

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.NettyBasedTierConsumer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.NettyBasedTierConsumerView;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.NettyBasedTierConsumerViewId;

import java.io.IOException;

/**
 * The {@link PartitionFileReader} interface defines the read logic for different types of shuffle
 * files.
 */
public interface PartitionFileReader {

    NettyBasedTierConsumer registerTierReader(
            int subpartitionId,
            NettyBasedTierConsumerViewId nettyBasedTierConsumerViewId,
            NettyBasedTierConsumerView tierConsumerView)
            throws IOException;

    void release();
}