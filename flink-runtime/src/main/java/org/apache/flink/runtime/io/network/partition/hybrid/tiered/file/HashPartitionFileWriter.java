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

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FatalExitExceptionHandler;
import org.apache.flink.util.concurrent.FutureUtils;

import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageUtils.createBaseSubpartitionPath;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageUtils.generateBufferWithHeaders;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageUtils.generateNewSegmentPath;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageUtils.writeSegmentFinishFile;
import static org.apache.flink.util.Preconditions.checkState;

/** THe implementation of {@link PartitionFileWriter} with merged logic. */
public class HashPartitionFileWriter implements PartitionFileWriter {

    private final ExecutorService ioExecutor =
            Executors.newSingleThreadExecutor(
                    new ThreadFactoryBuilder()
                            .setNameFormat("HashPartitionFileWriter flusher")
                            .setUncaughtExceptionHandler(FatalExitExceptionHandler.INSTANCE)
                            .build());

    private final JobID jobID;

    private final ResultPartitionID resultPartitionID;

    private final String baseShuffleDataPath;

    private final WritableByteChannel[] subpartitionChannels;

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
    public CompletableFuture<Void> write(
            List<PartitionFileWriter.SubpartitionSpilledBufferContext> spilledBuffers) {
        List<CompletableFuture<Void>> completableFutures = new ArrayList<>();
        spilledBuffers.forEach(
                subpartitionBuffers -> {
                    int subpartitionId = subpartitionBuffers.getSubpartitionId();
                    List<PartitionFileWriter.SegmentSpilledBufferContext> multiSegmentBuffers =
                            subpartitionBuffers.getSegmentSpillBufferContexts();
                    multiSegmentBuffers.forEach(
                            segmentBuffers -> {
                                CompletableFuture<Void> spillSuccessNotifier =
                                        new CompletableFuture<>();
                                Runnable writeRunnable =
                                        getWriteOrFinisheSegmentRunnable(
                                                subpartitionId,
                                                segmentBuffers,
                                                spillSuccessNotifier);
                                ioExecutor.execute(writeRunnable);
                                completableFutures.add(spillSuccessNotifier);
                            });
                });
        return FutureUtils.waitForAll(completableFutures);
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

    private Runnable getWriteOrFinisheSegmentRunnable(
            int subpartitionId,
            PartitionFileWriter.SegmentSpilledBufferContext segmentBuffers,
            CompletableFuture<Void> spillSuccessNotifier) {
        int segmentId = segmentBuffers.getSegmentId();
        List<SpilledBufferContext> spilledBuffers = segmentBuffers.getSpillBufferContexts();
        boolean isFinishSegment = segmentBuffers.needFinishSegment();
        checkState(!spilledBuffers.isEmpty() || isFinishSegment);

        Runnable writeRunnable;
        if (spilledBuffers.size() > 0) {
            writeRunnable =
                    () -> spill(subpartitionId, segmentId, spilledBuffers, spillSuccessNotifier);
        } else {
            writeRunnable =
                    () -> writeFinishSegmentFile(subpartitionId, segmentId, spillSuccessNotifier);
        }
        return writeRunnable;
    }

    /** Called in single-threaded ioExecutor. Order is guaranteed. */
    private void spill(
            int subpartitionId,
            int segmentId,
            List<SpilledBufferContext> spilledBuffers,
            CompletableFuture<Void> spillSuccessNotifier) {
        try {
            writeBuffers(
                    subpartitionId,
                    segmentId,
                    spilledBuffers,
                    createSpilledBuffersAndGetTotalBytes(spilledBuffers));
            spilledBuffers.forEach(spilledBuffer -> spilledBuffer.getBuffer().recycleBuffer());
            spillSuccessNotifier.complete(null);
        } catch (IOException exception) {
            ExceptionUtils.rethrow(exception);
        }
    }

    private long createSpilledBuffersAndGetTotalBytes(List<SpilledBufferContext> spilledBuffers) {
        long expectedBytes = 0;
        for (SpilledBufferContext spilledBuffer : spilledBuffers) {
            Buffer buffer = spilledBuffer.getBuffer();
            int numBytes = buffer.readableBytes() + BufferReaderWriterUtil.HEADER_LENGTH;
            expectedBytes += numBytes;
        }
        return expectedBytes;
    }

    private void writeBuffers(
            int subpartitionId,
            int segmentId,
            List<SpilledBufferContext> spilledBuffers,
            long expectedBytes)
            throws IOException {
        ByteBuffer[] bufferWithHeaders = generateBufferWithHeaders(spilledBuffers);
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
            TieredStorageUtils.writeDfsBuffers(currentChannel, expectedBytes, bufferWithHeaders);
            subpartitionChannels[subpartitionId] = currentChannel;
        } else {
            TieredStorageUtils.writeDfsBuffers(currentChannel, expectedBytes, bufferWithHeaders);
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
}
