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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.remote;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageUtils;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.PartitionFileIndex;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyPayload;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.CacheBufferSpiller;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageUtils.createBaseSubpartitionPath;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageUtils.generateBufferWithHeaders;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageUtils.generateNewSegmentPath;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageUtils.writeSegmentFinishFile;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * This component is responsible for asynchronously writing in-memory data to DFS file. Each
 * spilling operation will write the DFS file sequentially.
 */
public class RemoteCacheBufferSpiller implements CacheBufferSpiller {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteCacheBufferSpiller.class);

    private final ExecutorService ioExecutor;

    private final JobID jobID;

    private final ResultPartitionID resultPartitionID;

    private final int subpartitionId;

    private final String baseDfsPath;

    /** Records the current writing location. */
    private long totalBytesWritten;

    private WritableByteChannel writingChannel;

    private String baseSubpartitionPath;

    private boolean isSegmentStarted;

    private long currentSegmentIndex = -1;

    public RemoteCacheBufferSpiller(
            JobID jobID,
            ResultPartitionID resultPartitionID,
            int subpartitionId,
            String baseDfsPath,
            ExecutorService ioExecutor) {
        this.ioExecutor = ioExecutor;
        this.jobID = jobID;
        this.resultPartitionID = resultPartitionID;
        this.subpartitionId = subpartitionId;
        this.baseDfsPath = baseDfsPath;
    }

    @Override
    public void startSegment(int segmentIndex) {
        if (segmentIndex <= currentSegmentIndex) {
            return;
        }

        checkState(!isSegmentStarted);
        isSegmentStarted = true;
        currentSegmentIndex = segmentIndex;

        openNewSegmentFile();
    }

    @Override
    public CompletableFuture<Void> spillAsync(List<NettyPayload> bufferToSpill) {
        CompletableFuture<Void> spillSuccessNotifier = new CompletableFuture<>();
        ioExecutor.execute(() -> spill(bufferToSpill, spillSuccessNotifier));
        return spillSuccessNotifier;
    }

    @Override
    public void finishSegment(int segmentIndex) {
        checkState(currentSegmentIndex == segmentIndex);
        checkState(isSegmentStarted);

        closeCurrentSegmentFile();
        isSegmentStarted = false;
    }

    @Override
    public void release() {}

    @Override
    public void close() {
        closeWritingChannel();
    }

    private void openNewSegmentFile() {
        try {
            if (baseSubpartitionPath == null) {
                baseSubpartitionPath =
                        createBaseSubpartitionPath(
                                jobID, resultPartitionID, subpartitionId, baseDfsPath, false);
            }

            Path writingSegmentPath =
                    generateNewSegmentPath(baseSubpartitionPath, currentSegmentIndex);
            FileSystem fs = writingSegmentPath.getFileSystem();
            OutputStream outputStream =
                    fs.create(writingSegmentPath, FileSystem.WriteMode.OVERWRITE);
            writingChannel = Channels.newChannel(outputStream);
        } catch (IOException e) {
            throw new RuntimeException("Cannot open a new segment file.");
        }
    }

    private void closeCurrentSegmentFile() {
        checkState(writingChannel.isOpen(), "Writing channel is already closed.");
        closeWritingChannel();
        writeFinishSegmentFile();
    }

    /** Called in single-threaded ioExecutor. Order is guaranteed. */
    private void spill(List<NettyPayload> toWrite, CompletableFuture<Void> spillSuccessNotifier) {
        try {
            List<PartitionFileIndex.SpilledBuffer> spilledBuffers = new ArrayList<>();
            long expectedBytes = createSpilledBuffersAndGetTotalBytes(toWrite, spilledBuffers);
            // write all buffers to file
            writeBuffers(toWrite, expectedBytes);
            toWrite.forEach(buffer -> buffer.getBuffer().get().recycleBuffer());
            toWrite.clear();
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

    private void writeBuffers(List<NettyPayload> bufferWithIdentities, long expectedBytes)
            throws IOException {
        if (bufferWithIdentities.isEmpty()) {
            return;
        }
        ByteBuffer[] bufferWithHeaders = generateBufferWithHeaders(bufferWithIdentities);

        TieredStorageUtils.writeDfsBuffers(writingChannel, expectedBytes, bufferWithHeaders);
        totalBytesWritten += expectedBytes;
    }

    private void closeWritingChannel() {
        if (writingChannel != null && writingChannel.isOpen()) {
            try {
                writingChannel.close();
            } catch (Exception e) {
                LOG.warn("Failed to close writing segment channel.", e);
            }
        }
    }

    private void writeFinishSegmentFile() {
        checkState(baseSubpartitionPath != null, "Empty subpartition path.");
        writeSegmentFinishFile(baseSubpartitionPath, currentSegmentIndex);
    }

    @VisibleForTesting
    public String getBaseSubpartitionPath() {
        return baseSubpartitionPath;
    }
}
