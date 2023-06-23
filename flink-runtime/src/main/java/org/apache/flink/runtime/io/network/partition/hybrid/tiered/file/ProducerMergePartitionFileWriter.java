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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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

    /** One thread to perform the flush operation. */
    private final ExecutorService ioExecutor =
            Executors.newSingleThreadExecutor(
                    new ThreadFactoryBuilder()
                            .setNameFormat("Producer merge partition file flush thread")
                            .setUncaughtExceptionHandler(FatalExitExceptionHandler.INSTANCE)
                            .build());

    /** File channel to write data. */
    private final FileChannel dataFileChannel;

    /**
     * The partition file index. When flushing buffers, the partition file indexes will be updated.
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
            TieredStoragePartitionId partitionId, List<SubpartitionBufferContext> buffersToWrite) {
        CompletableFuture<Void> flushSuccessNotifier = new CompletableFuture<>();
        ioExecutor.execute(() -> flush(buffersToWrite, flushSuccessNotifier));
        return flushSuccessNotifier;
    }

    @Override
    public void release() {
        try {
            ioExecutor.shutdown();
            if (!ioExecutor.awaitTermination(5L, TimeUnit.MINUTES)) {
                throw new TimeoutException("Timeout to shutdown the flush thread.");
            }
            dataFileChannel.close();
        } catch (Exception e) {
            ExceptionUtils.rethrow(e);
        }
        partitionFileIndex.release();
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    /** Called in single-threaded ioExecutor. Order is guaranteed. */
    private void flush(
            List<SubpartitionBufferContext> toWrite, CompletableFuture<Void> flushSuccessNotifier) {
        try {
            List<PartitionFileIndex.BufferToFlush> toFlushBuffers = new ArrayList<>();
            calculateSizeAndFlushBuffers(toWrite, toFlushBuffers);
            partitionFileIndex.generateRegionsBasedOnBuffers(toFlushBuffers);
            flushSuccessNotifier.complete(null);
        } catch (IOException exception) {
            ExceptionUtils.rethrow(exception);
        }
    }

    /**
     * Compute buffer's file offset and create buffers to be flushed.
     *
     * @param toWrite all buffers to write to create {@link PartitionFileIndex.BufferToFlush}s
     * @param toFlushBuffers receive the created {@link PartitionFileIndex.BufferToFlush}
     */
    private void calculateSizeAndFlushBuffers(
            List<SubpartitionBufferContext> toWrite,
            List<PartitionFileIndex.BufferToFlush> toFlushBuffers)
            throws IOException {
        List<Tuple2<Buffer, Integer>> buffersToFlush = new ArrayList<>();
        long expectedBytes = 0;
        for (SubpartitionBufferContext subpartitionBufferContext : toWrite) {
            int subpartitionId = subpartitionBufferContext.getSubpartitionId();
            for (SegmentBufferContext segmentBufferContext :
                    subpartitionBufferContext.getSegmentBufferContexts()) {
                List<Tuple2<Buffer, Integer>> bufferWithIndexes =
                        segmentBufferContext.getBufferWithIndexes();
                buffersToFlush.addAll(bufferWithIndexes);
                for (Tuple2<Buffer, Integer> bufferWithIndex :
                        segmentBufferContext.getBufferWithIndexes()) {
                    Buffer buffer = bufferWithIndex.f0;
                    toFlushBuffers.add(
                            new PartitionFileIndex.BufferToFlush(
                                    subpartitionId,
                                    bufferWithIndex.f1,
                                    totalBytesWritten + expectedBytes));
                    expectedBytes += buffer.readableBytes() + BufferReaderWriterUtil.HEADER_LENGTH;
                }
            }
        }
        flushBuffers(buffersToFlush, expectedBytes);
        buffersToFlush.forEach(bufferWithIndex -> bufferWithIndex.f0.recycleBuffer());
    }

    /** Write all buffers to the disk. */
    private void flushBuffers(List<Tuple2<Buffer, Integer>> bufferWithIndexes, long expectedBytes)
            throws IOException {
        if (bufferWithIndexes.isEmpty()) {
            return;
        }

        ByteBuffer[] bufferWithHeaders = generateBufferWithHeaders(bufferWithIndexes);
        BufferReaderWriterUtil.writeBuffers(dataFileChannel, expectedBytes, bufferWithHeaders);
        totalBytesWritten += expectedBytes;
    }
}
