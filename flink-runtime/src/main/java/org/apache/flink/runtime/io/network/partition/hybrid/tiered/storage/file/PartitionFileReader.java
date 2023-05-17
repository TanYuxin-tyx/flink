package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file;

import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyServiceView;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyServiceViewId;

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
     * @param nettyServiceViewId the id of netty service view.
     * @param availabilityListener the availability listener of the reader.
     * @return the view of netty service
     * @throws IOException if the reader cannot register to netty service.
     */
    NettyServiceView registerNettyService(
            int subpartitionId,
            NettyServiceViewId nettyServiceViewId,
            BufferAvailabilityListener availabilityListener)
            throws IOException;

    /** Release the reader. */
    void release();
}
