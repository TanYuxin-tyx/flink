package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.service.NettyServiceViewId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.local.disk.RegionBufferIndexTracker;

import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkState;

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
    /** Return Long.MAX_VALUE if region does not exist to giving the lowest priority. */
    public long getFileOffset() {
        return currentBufferIndex == -1 ? Long.MAX_VALUE : offset;
    }

    public int getRemainingBuffersInRegion(
            int bufferIndex, NettyServiceViewId nettyServiceViewId) {
        updateCachedRegionIfNeeded(bufferIndex, nettyServiceViewId);

        return numReadable;
    }

    public void skipAll(long newOffset) {
        this.offset = newOffset;
        this.numSkip = 0;
    }

    /**
     * Maps the given buffer index to the offset in file.
     *
     * @return a tuple of {@code <numSkip,offset>}. The offset of the given buffer index can be
     *     derived by starting from the {@code offset} and skipping {@code numSkip} buffers.
     */
    public Tuple2<Integer, Long> getNumSkipAndFileOffset(int bufferIndex) {
        checkState(numSkip >= 0, "num skip must be greater than or equal to 0");
        // Assumption: buffer index is always requested / updated increasingly
        checkState(currentBufferIndex <= bufferIndex);
        return new Tuple2<>(numSkip, offset);
    }

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
