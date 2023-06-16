package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyPayload;
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
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    private final PartitionFileIndex partitionFileIndex;

    public ProducerMergePartitionFileWriter(
            Path dataFilePath, PartitionFileIndex partitionFileIndex) {
        LOG.info("Creating partition file " + dataFilePath);
        try {
            this.dataFileChannel =
                    FileChannel.open(
                            dataFilePath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create file channel.", e);
        }
        this.partitionFileIndex = partitionFileIndex;
    }

    @Override
    public CompletableFuture<Void> write(List<SubpartitionNettyPayload> toWriteBuffers) {
        List<NettyPayload> buffersToSpill =
                toWriteBuffers.stream()
                        .map(SubpartitionNettyPayload::getSegmentNettyPayloads)
                        .flatMap(
                                (Function<List<SegmentNettyPayload>, Stream<SegmentNettyPayload>>)
                                        Collection::stream)
                        .map(SegmentNettyPayload::getNettyPayloads)
                        .flatMap(
                                (Function<List<NettyPayload>, Stream<NettyPayload>>)
                                        Collection::stream)
                        .collect(Collectors.toList());

        CompletableFuture<Void> spillSuccessNotifier = new CompletableFuture<>();
        ioExecutor.execute(() -> spill(buffersToSpill, spillSuccessNotifier));
        return spillSuccessNotifier;
    }

    /** Called in single-threaded ioExecutor. Order is guaranteed. */
    private void spill(List<NettyPayload> toWrite, CompletableFuture<Void> spillSuccessNotifier) {
        try {
            List<PartitionFileIndex.SpilledBuffer> spilledBuffers = new ArrayList<>();
            long expectedBytes = createSpilledBuffersAndGetTotalBytes(toWrite, spilledBuffers);
            writeBuffers(toWrite, expectedBytes);
            partitionFileIndex.addRegionIndex(spilledBuffers);
            for (NettyPayload nettyPayload : toWrite) {
                nettyPayload.getBuffer().get().recycleBuffer();
            }
            spillSuccessNotifier.complete(null);
        } catch (IOException exception) {
            ExceptionUtils.rethrow(exception);
        }
    }

    /**
     * Compute buffer's file offset and create spilled buffers.
     *
     * @param toWrite for create {@link PartitionFileIndex.SpilledBuffer}.
     * @param spilledBuffers receive the created {@link PartitionFileIndex.SpilledBuffer} by this
     *     method.
     * @return total bytes(header size + buffer size) of all buffers to write.
     */
    private long createSpilledBuffersAndGetTotalBytes(
            List<NettyPayload> toWrite, List<PartitionFileIndex.SpilledBuffer> spilledBuffers) {
        long expectedBytes = 0;
        for (NettyPayload nettyPayload : toWrite) {
            Buffer buffer = nettyPayload.getBuffer().get();
            int numBytes = buffer.readableBytes() + BufferReaderWriterUtil.HEADER_LENGTH;
            spilledBuffers.add(
                    new PartitionFileIndex.SpilledBuffer(
                            nettyPayload.getSubpartitionId(),
                            nettyPayload.getBufferIndex(),
                            totalBytesWritten + expectedBytes));
            expectedBytes += numBytes;
        }
        return expectedBytes;
    }

    /** Write all buffers to disk. */
    private void writeBuffers(List<NettyPayload> nettyPayloads, long expectedBytes)
            throws IOException {
        if (nettyPayloads.isEmpty()) {
            return;
        }

        ByteBuffer[] bufferWithHeaders = generateBufferWithHeaders(nettyPayloads);

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
        partitionFileIndex.release();
    }
}
