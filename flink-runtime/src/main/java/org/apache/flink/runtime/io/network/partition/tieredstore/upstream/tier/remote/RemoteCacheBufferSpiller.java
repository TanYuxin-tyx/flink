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

package org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.remote;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.BufferWithIdentity;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.CacheBufferSpiller;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreUtils;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.RegionBufferIndexTracker;
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

import static org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreUtils.createBaseSubpartitionPath;
import static org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreUtils.generateBufferWithHeaders;
import static org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreUtils.generateNewSegmentPath;
import static org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreUtils.writeSegmentFinishFile;
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

    private Path writingSegmentPath;

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
    public void startSegment(long segmentIndex) throws IOException {
        if (segmentIndex <= currentSegmentIndex) {
            return;
        }

        checkState(!isSegmentStarted);
        isSegmentStarted = true;
        currentSegmentIndex = segmentIndex;

        openNewSegmentFile();
    }

    @Override
    public CompletableFuture<List<RegionBufferIndexTracker.SpilledBuffer>> spillAsync(
            List<BufferWithIdentity> bufferToSpill) {
        CompletableFuture<List<RegionBufferIndexTracker.SpilledBuffer>> spilledFuture =
                new CompletableFuture<>();
        ioExecutor.execute(() -> spill(bufferToSpill, spilledFuture));
        return spilledFuture;
    }

    @Override
    public void finishSegment(long segmentIndex) {
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

    private void openNewSegmentFile() throws IOException {
        if (baseSubpartitionPath == null) {
            baseSubpartitionPath =
                    createBaseSubpartitionPath(
                            jobID, resultPartitionID, subpartitionId, baseDfsPath, false);
        }

        writingSegmentPath = generateNewSegmentPath(baseSubpartitionPath, currentSegmentIndex);

        try {
            FileSystem fs = writingSegmentPath.getFileSystem();
            OutputStream outputStream =
                    fs.create(writingSegmentPath, FileSystem.WriteMode.OVERWRITE);
            writingChannel = Channels.newChannel(outputStream);
        } catch (IOException e) {
            ExceptionUtils.rethrow(e);
        }
    }

    private void closeCurrentSegmentFile() {
        checkState(writingChannel.isOpen(), "Writing channel is already closed.");
        closeWritingChannel();
        writeFinishSegmentFile();
    }

    /** Called in single-threaded ioExecutor. Order is guaranteed. */
    private void spill(
            List<BufferWithIdentity> toWrite,
            CompletableFuture<List<RegionBufferIndexTracker.SpilledBuffer>> spilledFuture) {
        try {
            List<RegionBufferIndexTracker.SpilledBuffer> spilledBuffers = new ArrayList<>();
            long expectedBytes = createSpilledBuffersAndGetTotalBytes(toWrite, spilledBuffers);
            // write all buffers to file
            writeBuffers(toWrite, expectedBytes);
            toWrite.forEach(buffer -> buffer.getBuffer().recycleBuffer());
            toWrite.clear();
            // complete spill future when buffers are written to disk successfully.
            // note that the ownership of these buffers is transferred to the MemoryDataManager,
            // which controls data's life cycle.
            spilledFuture.complete(spilledBuffers);
        } catch (IOException exception) {
            // if spilling is failed, throw exception directly to uncaughtExceptionHandler.
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
            List<BufferWithIdentity> toWrite,
            List<RegionBufferIndexTracker.SpilledBuffer> spilledBuffers) {
        long expectedBytes = 0;
        for (BufferWithIdentity bufferWithIdentity : toWrite) {
            Buffer buffer = bufferWithIdentity.getBuffer();
            int numBytes = buffer.readableBytes() + BufferReaderWriterUtil.HEADER_LENGTH;
            spilledBuffers.add(
                    new RegionBufferIndexTracker.SpilledBuffer(
                            bufferWithIdentity.getChannelIndex(),
                            bufferWithIdentity.getBufferIndex(),
                            totalBytesWritten + expectedBytes));
            expectedBytes += numBytes;
        }
        return expectedBytes;
    }

    private void writeBuffers(List<BufferWithIdentity> bufferWithIdentities, long expectedBytes)
            throws IOException {
        if (bufferWithIdentities.isEmpty()) {
            return;
        }
        ByteBuffer[] bufferWithHeaders = generateBufferWithHeaders(bufferWithIdentities);

        TieredStoreUtils.writeDfsBuffers(writingChannel, expectedBytes, bufferWithHeaders);
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
    public Path getBaseSubpartitionPath() {
        return new Path(baseSubpartitionPath);
    }
}
