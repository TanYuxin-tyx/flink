package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.ioscheduler;

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionWriter;

import java.io.IOException;

/**
 * The {@link DiskIOScheduler} interface defines the read logic for different types of shuffle
 * files.
 */
public interface DiskIOScheduler {

    /**
     * Register to netty service and provide buffer to transfer to down stream.
     *
     * @param subpartitionId the id of subpartition.
     * @param nettyConnectionWriter this writer is used to write netty buffers.
     * @return the view of netty service
     * @throws IOException if the reader cannot register to netty service.
     */
    void connectionEstablished(int subpartitionId, NettyConnectionWriter nettyConnectionWriter)
            throws IOException;

    void connectionBroken(NettyConnectionId id);

    /** Release the reader. */
    void release();
}
