package org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty;

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.netty2.NettyServiceWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.BufferContext;

import java.util.Queue;

public class NettyServiceWriterImpl implements NettyServiceWriter {

    private final Queue<BufferContext> bufferQueue;

    public NettyServiceWriterImpl(Queue<BufferContext> bufferQueue) {
        this.bufferQueue = bufferQueue;
    }

    @Override
    public void writeBuffer(BufferContext bufferContext) {
        bufferQueue.add(bufferContext);
    }

    @Override
    public void clear() {
        bufferQueue.clear();
    }
}
