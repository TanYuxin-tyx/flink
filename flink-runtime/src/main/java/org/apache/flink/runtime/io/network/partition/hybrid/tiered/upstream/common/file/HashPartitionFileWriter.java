package org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.file;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.BufferContext;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.TieredStoreUtils;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.tier.remote.RemoteCacheBufferSpiller;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FatalExitExceptionHandler;

import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.TieredStoreUtils.createBaseSubpartitionPath;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.TieredStoreUtils.generateBufferWithHeaders;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.TieredStoreUtils.generateNewSegmentPath;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.TieredStoreUtils.writeSegmentFinishFile;
import static org.apache.flink.util.Preconditions.checkState;

/** THe implementation of {@link PartitionFileWriter} with merged logic. */
public class HashPartitionFileWriter implements PartitionFileWriter {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteCacheBufferSpiller.class);

    private final ExecutorService ioExecutor =
            Executors.newSingleThreadExecutor(
                    new ThreadFactoryBuilder()
                            .setNameFormat("HashPartitionFileWriter flusher")
                            .setUncaughtExceptionHandler(FatalExitExceptionHandler.INSTANCE)
                            .build());

    private final JobID jobID;

    private final ResultPartitionID resultPartitionID;

    private final String baseShuffleDataPath;

    private volatile WritableByteChannel[] subpartitionChannels;

    public HashPartitionFileWriter(
            JobID jobID,
            int numSubpartitions,
            ResultPartitionID resultPartitionID,
            String baseShuffleDataPath) {
        this.jobID = jobID;
        this.resultPartitionID = resultPartitionID;
        this.baseShuffleDataPath = baseShuffleDataPath;
        this.subpartitionChannels = new WritableByteChannel[numSubpartitions];
        Arrays.fill(subpartitionChannels, null);
    }

    @Override
    public CompletableFuture<Void> spillAsync(List<BufferContext> bufferToSpill) {
        // nothing to do.
        return null;
    }

    @Override
    public CompletableFuture<Void> spillAsync(
            int subpartitionId, int segmentId, List<BufferContext> bufferToSpill) {
        checkState(bufferToSpill.size() > 0);
        CompletableFuture<Void> spillSuccessNotifier = new CompletableFuture<>();
        ioExecutor.execute(
                () -> spill(subpartitionId, segmentId, bufferToSpill, spillSuccessNotifier));
        return spillSuccessNotifier;
    }

    @Override
    public CompletableFuture<Void> finishSegment(int subpartitionId, int segmentId) {
        CompletableFuture<Void> spillSuccessNotifier = new CompletableFuture<>();
        ioExecutor.execute(
                () -> writeFinishSegmentFile(subpartitionId, segmentId, spillSuccessNotifier));
        return spillSuccessNotifier;
    }

    @Override
    public void release() {
        try {
            ioExecutor.shutdown();
            if (!ioExecutor.awaitTermination(5L, TimeUnit.MINUTES)) {
                throw new TimeoutException("Shutdown spilling thread timeout.");
            }
        } catch (Exception e) {
            ExceptionUtils.rethrow(e);
        }
    }

    /** Called in single-threaded ioExecutor. Order is guaranteed. */
    private void spill(
            int subpartitionId,
            int segmentId,
            List<BufferContext> toWrite,
            CompletableFuture<Void> spillSuccessNotifier) {
        try {
            writeBuffers(
                    subpartitionId,
                    segmentId,
                    toWrite,
                    createSpilledBuffersAndGetTotalBytes(toWrite));
            toWrite.forEach(buffer -> buffer.getBuffer().recycleBuffer());
            spillSuccessNotifier.complete(null);
        } catch (IOException exception) {
            ExceptionUtils.rethrow(exception);
        }
    }

    private long createSpilledBuffersAndGetTotalBytes(
            List<BufferContext> toWriteSubpartitionBuffers) {
        long expectedBytes = 0;
        for (BufferContext bufferContext : toWriteSubpartitionBuffers) {
            Buffer buffer = bufferContext.getBuffer();
            int numBytes = buffer.readableBytes() + BufferReaderWriterUtil.HEADER_LENGTH;
            expectedBytes += numBytes;
        }
        return expectedBytes;
    }

    private void writeBuffers(
            int subpartitionId, int segmentId, List<BufferContext> toWrite, long expectedBytes)
            throws IOException {
        ByteBuffer[] bufferWithHeaders = generateBufferWithHeaders(toWrite);
        WritableByteChannel currentChannel = subpartitionChannels[subpartitionId];
        if (currentChannel == null) {
            String subpartitionPath =
                    createBaseSubpartitionPath(
                            jobID, resultPartitionID, subpartitionId, baseShuffleDataPath, false);
            Path writingSegmentPath = generateNewSegmentPath(subpartitionPath, segmentId);
            FileSystem fs = writingSegmentPath.getFileSystem();
            currentChannel =
                    Channels.newChannel(
                            fs.create(writingSegmentPath, FileSystem.WriteMode.NO_OVERWRITE));
            TieredStoreUtils.writeDfsBuffers(currentChannel, expectedBytes, bufferWithHeaders);
            subpartitionChannels[subpartitionId] = currentChannel;
        } else {
            TieredStoreUtils.writeDfsBuffers(currentChannel, expectedBytes, bufferWithHeaders);
        }
    }

    private void writeFinishSegmentFile(
            int subpartitionId, int segmentId, CompletableFuture<Void> spillSuccessNotifier) {
        String subpartitionPath = null;
        try {
            subpartitionPath =
                    createBaseSubpartitionPath(
                            jobID, resultPartitionID, subpartitionId, baseShuffleDataPath, false);
        } catch (IOException exception) {
            ExceptionUtils.rethrow(exception);
        }
        writeSegmentFinishFile(subpartitionPath, segmentId);
        // clear the current channel
        subpartitionChannels[subpartitionId] = null;
        spillSuccessNotifier.complete(null);
    }

    @VisibleForTesting
    public String getBaseSubpartitionPath(int subpartitionId) {
        return "";
    }
}
