package org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.file;

import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierReader;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierReaderView;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierReaderViewId;

import java.io.IOException;

/**
 * The {@link PartitionFileReader} interface defines the read logic for different types of shuffle
 * files.
 */
public interface PartitionFileReader {

    TierReader registerTierReader(
            int subpartitionId, TierReaderViewId tierReaderViewId, TierReaderView tierReaderView)
            throws IOException;

    void release();
}
