package org.apache.flink.runtime.io.network.partition.store.history.memory;

import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.store.common.SingleTierReader;

import javax.annotation.Nullable;

import java.io.IOException;

/** The read view of {@link LocalMemoryDataManager}, data can be read from memory. */
public class MemoryReader implements SingleTierReader {

    private final ResultSubpartitionView subpartitionView;

    public MemoryReader(ResultSubpartitionView subpartitionView) {
        this.subpartitionView = subpartitionView;
    }

    @Nullable
    @Override
    public ResultSubpartition.BufferAndBacklog getNextBuffer() throws IOException {
        return subpartitionView.getNextBuffer();
    }

    @Override
    public void releaseAllResources() throws IOException {
        subpartitionView.releaseAllResources();
    }

    @Override
    public boolean isReleased() {
        return subpartitionView.isReleased();
    }

    @Override
    public ResultSubpartitionView.AvailabilityWithBacklog getAvailabilityAndBacklog(
            int numCreditsAvailable) {
        return subpartitionView.getAvailabilityAndBacklog(numCreditsAvailable);
    }

    @Override
    public Throwable getFailureCause() {
        return subpartitionView.getFailureCause();
    }

    @Override
    public int unsynchronizedGetNumberOfQueuedBuffers() {
        return subpartitionView.unsynchronizedGetNumberOfQueuedBuffers();
    }

    @Override
    public int getNumberOfQueuedBuffers() {
        return subpartitionView.getNumberOfQueuedBuffers();
    }

    @Override
    public String toString() {
        return subpartitionView.toString();
    }
}
