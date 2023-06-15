package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyPayload;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * The {@link PartitionFileWriter} interface defines the write logic for different types of shuffle
 * files.
 */
public interface PartitionFileWriter {

    CompletableFuture<Void> write(
            List<Tuple2<Integer, Tuple3<Integer, List<NettyPayload>, Boolean>>> toWriteBuffers);

    CompletableFuture<Void> finishSegment(int subpartitionId, int segmentId);

    void release();
}
