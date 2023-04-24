package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.service.NettyServiceViewId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.local.disk.RegionBufferIndexTracker;

import java.util.Optional;

/** TODO nothing. */
public class CachedRegionManager {

    private final int subpartitionId;
    private final RegionBufferIndexTracker dataIndex;

    private int currentBufferIndex;
    private int numSkip;
    private int numReadable;
    private long offset;

    public CachedRegionManager(int subpartitionId, RegionBufferIndexTracker dataIndex) {
        this.subpartitionId = subpartitionId;
        this.dataIndex = dataIndex;
    }

    // 获取档前文件描述符的 file offset
    public long getFileOffset() {
        return currentBufferIndex == -1 ? Long.MAX_VALUE : offset;
    }

    // 获取当前region还剩几个buffer
    public int getRemainingBuffersInRegion(int bufferIndex, NettyServiceViewId nettyServiceViewId) {
        updateCachedRegionIfNeeded(bufferIndex, nettyServiceViewId);
        return numReadable;
    }

    // 更新 当前的offset
    public void skipAll(long newOffset) {
        this.offset = newOffset;
        this.numSkip = 0;
    }

    public Tuple2<Integer, Long> getNumSkipAndFileOffset() {
        return new Tuple2<>(numSkip, offset);
    }

    // 更新掉自己的offset
    public void advance(long bufferSize) {
        if (isInCachedRegion(currentBufferIndex + 1)) {
            currentBufferIndex++;
            numReadable--;
            offset += bufferSize;
        }
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    /** Points the cursors to the given buffer index, if possible. */
    private void updateCachedRegionIfNeeded(
            int bufferIndex, NettyServiceViewId nettyServiceViewId) {
        if (isInCachedRegion(bufferIndex)) {
            int numAdvance = bufferIndex - currentBufferIndex;
            numSkip += numAdvance;
            numReadable -= numAdvance;
            currentBufferIndex = bufferIndex;
            return;
        }

        Optional<RegionBufferIndexTracker.ReadableRegion> lookupResultOpt =
                dataIndex.getReadableRegion(subpartitionId, bufferIndex, nettyServiceViewId);
        if (!lookupResultOpt.isPresent()) {
            currentBufferIndex = -1;
            numReadable = 0;
            numSkip = 0;
            offset = -1L;
        } else {
            RegionBufferIndexTracker.ReadableRegion cachedRegion = lookupResultOpt.get();
            currentBufferIndex = bufferIndex;
            numSkip = cachedRegion.numSkip;
            numReadable = cachedRegion.numReadable;
            offset = cachedRegion.offset;
        }
    }

    private boolean isInCachedRegion(int bufferIndex) {
        return bufferIndex < currentBufferIndex + numReadable && bufferIndex >= currentBufferIndex;
    }
}
