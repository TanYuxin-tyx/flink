package org.apache.flink.runtime.io.network.partition.tieredstore.downstream;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.tieredstore.TieredStoreMode;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreMemoryManager;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.runtime.shuffle.NettyShuffleUtils.HYBRID_SHUFFLE_TIER_EXCLUSIVE_BUFFERS;

/** Upstream tasks will get buffer from this {@link DownstreamTieredStoreMemoryManager}. */
public class DownstreamTieredStoreMemoryManager implements TieredStoreMemoryManager {

    private final BufferPool localBufferPool;

    private final AtomicInteger numRequestedBuffers = new AtomicInteger(0);

    public DownstreamTieredStoreMemoryManager(NetworkBufferPool networkBufferPool) {
        int numExclusive =
                HYBRID_SHUFFLE_TIER_EXCLUSIVE_BUFFERS.get(TieredStoreMode.TierType.IN_REMOTE);
        try {
            this.localBufferPool = networkBufferPool.createBufferPool(numExclusive, numExclusive);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create localBufferPool", e);
        }
    }

    @Override
    public int numAvailableBuffers(TieredStoreMode.TierType tierType) {
        return localBufferPool.getNumberOfAvailableMemorySegments();
    }

    @Override
    public int numRequestedBuffers() {
        return numRequestedBuffers.get();
    }

    @Override
    public int numTotalBuffers() {
        return localBufferPool.getNumBuffers();
    }

    @Override
    public float numBuffersTriggerFlushRatio() {
        return 0;
    }

    @Override
    public MemorySegment requestMemorySegmentBlocking(TieredStoreMode.TierType tierType) {
        try {
            return localBufferPool.requestMemorySegmentBlocking();
        } catch (InterruptedException e) {
            throw new RuntimeException("Failed to request memory segments.", e);
        }
    }

    @Override
    public void recycleBuffer(MemorySegment memorySegment, TieredStoreMode.TierType tierType) {
        localBufferPool.recycle(memorySegment);
    }

    @Override
    public void incNumRequestedBuffer(TieredStoreMode.TierType tierType) {}

    @Override
    public void decNumRequestedBuffer(TieredStoreMode.TierType tierType) {}

    @Override
    public void checkNeedTriggerFlushCachedBuffers() {}

    @Override
    public void close() {
        // nothing to do.
    }

    @Override
    public void release() {}
}
