/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.file;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;
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
    public CompletableFuture<Void> write(
            List<PartitionFileWriter.SubpartitionSpilledBufferContext> spilledBuffers) {
        List<SpilledBufferContext> buffersToSpill =
                spilledBuffers.stream()
                        .map(SubpartitionSpilledBufferContext::getSegmentSpillBufferContexts)
                        .flatMap(
                                (Function<
                                                List<SegmentSpilledBufferContext>,
                                                Stream<SegmentSpilledBufferContext>>)
                                        Collection::stream)
                        .map(SegmentSpilledBufferContext::getSpillBufferContexts)
                        .flatMap(
                                (Function<List<SpilledBufferContext>, Stream<SpilledBufferContext>>)
                                        Collection::stream)
                        .collect(Collectors.toList());

        CompletableFuture<Void> spillSuccessNotifier = new CompletableFuture<>();
        ioExecutor.execute(() -> spill(buffersToSpill, spillSuccessNotifier));
        return spillSuccessNotifier;
    }

    /** Called in single-threaded ioExecutor. Order is guaranteed. */
    private void spill(
            List<SpilledBufferContext> toWrite, CompletableFuture<Void> spillSuccessNotifier) {
        try {
            List<PartitionFileIndex.SpilledBuffer> spilledBuffers = new ArrayList<>();
            long expectedBytes = createSpilledBuffersAndGetTotalBytes(toWrite, spilledBuffers);
            writeBuffers(toWrite, expectedBytes);
            partitionFileIndex.addRegionForBuffers(spilledBuffers);
            toWrite.forEach(spilledBuffer -> spilledBuffer.getBuffer().recycleBuffer());
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
            List<SpilledBufferContext> toWrite,
            List<PartitionFileIndex.SpilledBuffer> spilledBuffers) {
        long expectedBytes = 0;
        for (SpilledBufferContext spilledBuffer : toWrite) {
            Buffer buffer = spilledBuffer.getBuffer();
            int numBytes = buffer.readableBytes() + BufferReaderWriterUtil.HEADER_LENGTH;
            spilledBuffers.add(
                    new PartitionFileIndex.SpilledBuffer(
                            spilledBuffer.getSubpartitionId(),
                            spilledBuffer.getBufferIndex(),
                            totalBytesWritten + expectedBytes));
            expectedBytes += numBytes;
        }
        return expectedBytes;
    }

    /** Write all buffers to disk. */
    private void writeBuffers(List<SpilledBufferContext> nettyPayloads, long expectedBytes)
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
