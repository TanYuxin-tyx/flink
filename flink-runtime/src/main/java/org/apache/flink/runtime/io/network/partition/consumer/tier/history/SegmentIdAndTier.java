package org.apache.flink.runtime.io.network.partition.consumer.tier.history;

/** Recording a segment of data. */
public class SegmentIdAndTier {
    private final long segmentId;
    private final int tier;

    public SegmentIdAndTier(long segmentId, int tier) {
        this.segmentId = segmentId;
        this.tier = tier;
    }

    public long getSegmentId() {
        return segmentId;
    }

    public int getTier() {
        return tier;
    }
}
