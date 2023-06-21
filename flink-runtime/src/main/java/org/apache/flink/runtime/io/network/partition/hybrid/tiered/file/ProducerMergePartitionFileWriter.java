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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
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

/**
 * The implementation of {@link PartitionFileWriter} with producer-side merge mode. In this mode,
 * the shuffle data is written in the producer side, the consumer side need to read multiple
 * producers to get its partition data.
 *
 * <p>Note that one partition file written by the {@link ProducerMergePartitionFileWriter} may
 * contain the data of multiple subpartitions.
 */
public class ProducerMergePartitionFileWriter implements PartitionFileWriter {

    private static final Logger LOG =
            LoggerFactory.getLogger(ProducerMergePartitionFileWriter.class);

    /** One thread to perform the spill operation. */
    private final ExecutorService ioExecutor =
            Executors.newSingleThreadExecutor(
                    new ThreadFactoryBuilder()
                            .setNameFormat("ProducerMergePartitionFileWriter Spiller")
                            .setUncaughtExceptionHandler(FatalExitExceptionHandler.INSTANCE)
                            .build());

    /** File channel to write data. */
    private final FileChannel dataFileChannel;

    /**
     * The partition file index. When spilling buffers, the partition file indexes will be updated.
     */
    private final PartitionFileIndex partitionFileIndex;

    /** Records the current writing location. */
    private long totalBytesWritten;

    ProducerMergePartitionFileWriter(Path dataFilePath, PartitionFileIndex partitionFileIndex) {
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
            TieredStoragePartitionId partitionId,
            List<PartitionFileWriter.SubpartitionSpilledBufferContext> spilledBuffers) {
        List<Tuple2<Buffer, Integer>> buffersToSpill =
                spilledBuffers.stream()
                        .map(SubpartitionSpilledBufferContext::getSegmentSpillBufferContexts)
                        .flatMap(
                                (Function<
                                                List<SegmentSpilledBufferContext>,
                                                Stream<SegmentSpilledBufferContext>>)
                                        Collection::stream)
                        .map(SegmentSpilledBufferContext::getBufferWithIndexes)
                        .flatMap(
                                (Function<
                                                List<Tuple2<Buffer, Integer>>,
                                                Stream<Tuple2<Buffer, Integer>>>)
                                        Collection::stream)
                        .collect(Collectors.toList());

        CompletableFuture<Void> spillSuccessNotifier = new CompletableFuture<>();
        ioExecutor.execute(() -> spill(spilledBuffers, spillSuccessNotifier));
        return spillSuccessNotifier;
    }

    /** Called in single-threaded ioExecutor. Order is guaranteed. */
    private void spill(
            List<PartitionFileWriter.SubpartitionSpilledBufferContext> toWrite,
            CompletableFuture<Void> spillSuccessNotifier) {
        try {
            List<PartitionFileIndex.SpilledBuffer> spilledBuffers = new ArrayList<>();
            calculateSizeAndWriteBuffers(toWrite, spilledBuffers);
            partitionFileIndex.generateRegionsBasedOnBuffers(spilledBuffers);
            spillSuccessNotifier.complete(null);
        } catch (IOException exception) {
            ExceptionUtils.rethrow(exception);
        }
    }

    /**
     * Compute buffer's file offset and create spilled buffers.
     *
     * @param toWrite all buffers to write to create {@link PartitionFileIndex.SpilledBuffer}s
     * @param spilledBuffers receive the created {@link PartitionFileIndex.SpilledBuffer}
     */
    private void calculateSizeAndWriteBuffers(
            List<PartitionFileWriter.SubpartitionSpilledBufferContext> toWrite,
            List<PartitionFileIndex.SpilledBuffer> spilledBuffers)
            throws IOException {
        List<Tuple2<Buffer, Integer>> buffersToFlush = new ArrayList<>();
        long expectedBytes = 0;
        for (PartitionFileWriter.SubpartitionSpilledBufferContext subpartitionSpilledBufferContext :
                toWrite) {
            int subpartitionId = subpartitionSpilledBufferContext.getSubpartitionId();
            for (PartitionFileWriter.SegmentSpilledBufferContext segmentSpilledBufferContext :
                    subpartitionSpilledBufferContext.getSegmentSpillBufferContexts()) {
                List<Tuple2<Buffer, Integer>> bufferWithIndexes =
                        segmentSpilledBufferContext.getBufferWithIndexes();
                buffersToFlush.addAll(bufferWithIndexes);
                for (Tuple2<Buffer, Integer> bufferWithIndex :
                        segmentSpilledBufferContext.getBufferWithIndexes()) {
                    Buffer buffer = bufferWithIndex.f0;
                    spilledBuffers.add(
                            new PartitionFileIndex.SpilledBuffer(
                                    subpartitionId,
                                    bufferWithIndex.f1,
                                    totalBytesWritten + expectedBytes));
                    expectedBytes += buffer.readableBytes() + BufferReaderWriterUtil.HEADER_LENGTH;
                }
            }
        }
        writeBuffers(buffersToFlush, expectedBytes);
        buffersToFlush.forEach(bufferWithIndex -> bufferWithIndex.f0.recycleBuffer());
    }

    /** Write all buffers to disk. */
    private void writeBuffers(List<Tuple2<Buffer, Integer>> bufferWithIndexes, long expectedBytes)
            throws IOException {
        if (bufferWithIndexes.isEmpty()) {
            return;
        }

        ByteBuffer[] bufferWithHeaders = generateBufferWithHeaders(bufferWithIndexes);
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
