package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.BufferContext;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.local.disk.RegionBufferIndexTracker;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FatalExitExceptionHandler;

import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageUtils.generateBufferWithHeaders;

/** THe implementation of {@link PartitionFileWriter} with merged logic. */
public class ProducerMergePartitionFileWriter implements PartitionFileWriter {

    private static final Logger LOG =
            LoggerFactory.getLogger(ProducerMergePartitionFileWriter.class);

    /** One thread to perform spill operation. */
    private final ExecutorService ioExecutor =
            Executors.newSingleThreadExecutor(
                    new ThreadFactoryBuilder()
                            .setNameFormat("ProducerMergePartitionFileWriter Spiller")
                            .setUncaughtExceptionHandler(FatalExitExceptionHandler.INSTANCE)
                            .build());

    /** File channel to write data. */
    private final FileChannel dataFileChannel;

    /** Records the current writing location. */
    private long totalBytesWritten;

    private final RegionBufferIndexTracker regionBufferIndexTracker;

    public ProducerMergePartitionFileWriter(
            Path dataFilePath, RegionBufferIndexTracker regionBufferIndexTracker) {
        LOG.info("Creating partition file " + dataFilePath);
        try {
            this.dataFileChannel =
                    FileChannel.open(
                            dataFilePath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create file channel.", e);
        }
        this.regionBufferIndexTracker = regionBufferIndexTracker;
    }

    @Override
    public CompletableFuture<Void> spillAsync(
            int subpartitionId, int segmentId, List<BufferContext> bufferToSpill) {
        // nothing to do.
        return null;
    }

    @Override
    public CompletableFuture<Void> finishSegment(int subpartitionId, int segmentId) {
        // nothing to do.
        return null;
    }

    @Override
    public CompletableFuture<Void> spillAsync(List<BufferContext> bufferToSpill) {
        CompletableFuture<Void> spillSuccessNotifier = new CompletableFuture<>();
        ioExecutor.execute(() -> spill(bufferToSpill, spillSuccessNotifier));
        return spillSuccessNotifier;
    }

    /** Called in single-threaded ioExecutor. Order is guaranteed. */
    private void spill(List<BufferContext> toWrite, CompletableFuture<Void> spillSuccessNotifier) {
        try {
            List<RegionBufferIndexTracker.SpilledBuffer> spilledBuffers = new ArrayList<>();
            long expectedBytes = createSpilledBuffersAndGetTotalBytes(toWrite, spilledBuffers);
            writeBuffers(toWrite, expectedBytes);
            regionBufferIndexTracker.addBuffers(spilledBuffers);
            for (BufferContext bufferContext : toWrite) {
                bufferContext.getBuffer().recycleBuffer();
            }
            spillSuccessNotifier.complete(null);
        } catch (IOException exception) {
            ExceptionUtils.rethrow(exception);
        }
    }

    /**
     * Compute buffer's file offset and create spilled buffers.
     *
     * @param toWrite for create {@link RegionBufferIndexTracker.SpilledBuffer}.
     * @param spilledBuffers receive the created {@link RegionBufferIndexTracker.SpilledBuffer} by
     *     this method.
     * @return total bytes(header size + buffer size) of all buffers to write.
     */
    private long createSpilledBuffersAndGetTotalBytes(
            List<BufferContext> toWrite,
            List<RegionBufferIndexTracker.SpilledBuffer> spilledBuffers) {
        long expectedBytes = 0;
        for (BufferContext bufferContext : toWrite) {
            Buffer buffer = bufferContext.getBuffer();
            int numBytes = buffer.readableBytes() + BufferReaderWriterUtil.HEADER_LENGTH;
            spilledBuffers.add(
                    new RegionBufferIndexTracker.SpilledBuffer(
                            bufferContext.getSubpartitionId(),
                            bufferContext.getBufferIndex(),
                            totalBytesWritten + expectedBytes));
            expectedBytes += numBytes;
        }
        return expectedBytes;
    }

    /** Write all buffers to disk. */
    private void writeBuffers(List<BufferContext> bufferContexts, long expectedBytes)
            throws IOException {
        if (bufferContexts.isEmpty()) {
            return;
        }

        ByteBuffer[] bufferWithHeaders = generateBufferWithHeaders(bufferContexts);

        BufferReaderWriterUtil.writeBuffers(dataFileChannel, expectedBytes, bufferWithHeaders);
        totalBytesWritten += expectedBytes;
    }

    /**
     * Release this {@link ProducerMergePartitionFileWriter} when resultPartition is released. It
     * means spiller will wait for all previous spilling operation done blocking and close the file
     * channel.
     *
     * <p>This method only called by rpc thread.
     */
    @Override
    public void release() {
        try {
            ioExecutor.shutdown();
            if (!ioExecutor.awaitTermination(5L, TimeUnit.MINUTES)) {
                throw new TimeoutException("Shutdown spilling thread timeout.");
            }
            dataFileChannel.close();
        } catch (Exception e) {
            ExceptionUtils.rethrow(e);
        }
        regionBufferIndexTracker.release();
    }
}
