package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyServiceView;

import java.io.IOException;
import java.util.Queue;

/**
 * The {@link ProducerMergePartitionSubpartitionReader} is responded to load buffers of a
 * subpartition from disk.
 */
public interface ProducerMergePartitionSubpartitionReader extends Comparable<ProducerMergePartitionSubpartitionReader>{

    /**
     * Loaad data belonging to the specific subpartition to buffers.
     *
     * @param buffers is available memory segments.
     * @param recycler is the recycler of buffer.
     */
    void readBuffers(Queue<MemorySegment> buffers, BufferRecycler recycler) throws IOException;


    void setNettyServiceView(NettyServiceView nettyServiceView);

    /**
     * Fail the subpartition reader with the throwable.
     *
     * @param failureCause is the cause of failure.
     */
    void fail(Throwable failureCause);

    long getNextOffsetToLoad();
}
