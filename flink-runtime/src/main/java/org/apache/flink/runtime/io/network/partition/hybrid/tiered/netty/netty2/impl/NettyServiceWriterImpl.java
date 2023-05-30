package org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.netty2.impl;

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.netty2.NettyServiceWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.BufferContext;

import java.util.Queue;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class NettyServiceWriterImpl implements NettyServiceWriter {

    private final Queue<BufferContext> bufferQueue;

    public NettyServiceWriterImpl(Queue<BufferContext> bufferQueue) {
        this.bufferQueue = bufferQueue;
    }

    @Override
    public int size() {
        return bufferQueue.size();
    }

    @Override
    public void writeBuffer(BufferContext bufferContext) {
        bufferQueue.add(bufferContext);
    }

    @Override
    public void clear() {
        BufferContext bufferContext;
        while ((bufferContext = bufferQueue.poll()) != null) {
            if (bufferContext.getBuffer() != null) {
                checkNotNull(bufferContext.getBuffer()).recycleBuffer();
            }
        }
    }
}
