package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file;

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.netty2.NettyServiceWriterId;

import java.io.IOException;

/**
 * The {@link PartitionFileReader} interface defines the read logic for different types of shuffle
 * files.
 */
public interface PartitionFileReader {

    /**
     * Register to netty service and provide buffer to transfer to down stream.
     *
     * @param subpartitionId the id of subpartition.
     * @param nettyServiceWriterId the id of netty service view.
     * @param availabilityListener the availability listener of the reader.
     * @return the view of netty service
     * @throws IOException if the reader cannot register to netty service.
     */
    void registerNettyService(
            int subpartitionId,
            NettyServiceWriterId nettyServiceWriterId)
            throws IOException;

    /** Release the reader. */
    void release();
}
