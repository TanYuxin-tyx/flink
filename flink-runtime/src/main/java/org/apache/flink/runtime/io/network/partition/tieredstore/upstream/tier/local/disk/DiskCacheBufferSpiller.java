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

package org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.BufferWithIdentity;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.CacheBufferSpiller;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreUtils.generateBufferWithHeaders;

/**
 * This component is responsible for asynchronously writing in-memory data to disk. Each spilling
 * operation will write the disk file sequentially.
 */
public class DiskCacheBufferSpiller implements CacheBufferSpiller {

    private static final Logger LOG = LoggerFactory.getLogger(DiskCacheBufferSpiller.class);

    /** One thread to perform spill operation. */
    private final ExecutorService ioExecutor =
            Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder()
                            .setNameFormat("tiered store local file spiller")
                            // It is more appropriate to use task fail over than exit JVM here,
                            // but the task thread will bring some extra overhead to check the
                            // exception information set by other thread. As the spiller thread will
                            // not encounter exceptions in most cases, we temporarily choose the
                            // form of fatal error to deal except thrown by spiller thread.
                            .setUncaughtExceptionHandler(FatalExitExceptionHandler.INSTANCE)
                            .build());

    /** File channel to write data. */
    private final FileChannel dataFileChannel;

    /** Records the current writing location. */
    private long totalBytesWritten;

    private final RegionBufferIndexTracker regionBufferIndexTracker;

    public DiskCacheBufferSpiller(
            Path dataFilePath, RegionBufferIndexTracker regionBufferIndexTracker)
            throws IOException {
        LOG.info("Creating partition file " + dataFilePath);
        this.dataFileChannel =
                FileChannel.open(
                        dataFilePath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        this.regionBufferIndexTracker = regionBufferIndexTracker;
    }

    @Override
    public void startSegment(int segmentIndex) {}

    /**
     * Spilling buffers to disk asynchronously.
     *
     * @param bufferToSpill buffers need to be spilled, must ensure that it is sorted by
     *     (subpartitionId, bufferIndex).
     * @return the completable future contains spilled buffers information.
     */
    @Override
    public CompletableFuture<List<RegionBufferIndexTracker.SpilledBuffer>> spillAsync(
            List<BufferWithIdentity> bufferToSpill) {
        CompletableFuture<List<RegionBufferIndexTracker.SpilledBuffer>> spilledFuture =
                new CompletableFuture<>();
        ioExecutor.execute(() -> spill(bufferToSpill, spilledFuture));
        return spilledFuture;
    }

    @Override
    public void spillAsync(
            List<BufferWithIdentity> bufferToSpill,
            AtomicInteger hasFlushCompleted,
            boolean changeFlushState) {
        ioExecutor.execute(
                () ->
                        spill(
                                bufferToSpill,
                                hasFlushCompleted,
                                changeFlushState));
    }

    @Override
    public void finishSegment(int segmentIndex) {}

    /** Called in single-threaded ioExecutor. Order is guaranteed. */
    private void spill(
            List<BufferWithIdentity> toWrite,
            CompletableFuture<List<RegionBufferIndexTracker.SpilledBuffer>> spilledFuture) {
        try {
            List<RegionBufferIndexTracker.SpilledBuffer> spilledBuffers = new ArrayList<>();
            long expectedBytes = createSpilledBuffersAndGetTotalBytes(toWrite, spilledBuffers);
            // write all buffers to file
            writeBuffers(toWrite, expectedBytes);
            // complete spill future when buffers are written to disk successfully.
            // note that the ownership of these buffers is transferred to the MemoryDataManager,
            // which controls data's life cycle.
            regionBufferIndexTracker.addBuffers(spilledBuffers);
            spilledFuture.complete(null);
        } catch (IOException exception) {
            // if spilling is failed, throw exception directly to uncaughtExceptionHandler.
            ExceptionUtils.rethrow(exception);
        }
    }

    /** Called in single-threaded ioExecutor. Order is guaranteed. */
    private void spill(
            List<BufferWithIdentity> toWrite,
            AtomicInteger hasFlushCompleted,
            boolean changeFlushState) {
        try {
            List<RegionBufferIndexTracker.SpilledBuffer> spilledBuffers = new ArrayList<>();
            long expectedBytes = createSpilledBuffersAndGetTotalBytes(toWrite, spilledBuffers);
            // write all buffers to file
            writeBuffers(toWrite, expectedBytes);
            // complete spill future when buffers are written to disk successfully.
            // note that the ownership of these buffers is transferred to the MemoryDataManager,
            // which controls data's life cycle.
            regionBufferIndexTracker.addBuffers(spilledBuffers);
            for (BufferWithIdentity bufferWithIdentity : toWrite) {
                regionBufferIndexTracker.markBufferReleased(
                        bufferWithIdentity.getChannelIndex(), bufferWithIdentity.getBufferIndex());
                bufferWithIdentity.getBuffer().recycleBuffer();
            }
            //spillingCompleteFuture.complete(null);
            if (changeFlushState) {
                hasFlushCompleted.decrementAndGet();
            }
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

    /** Write all buffers to disk. */
    private void writeBuffers(List<BufferWithIdentity> bufferWithIdentities, long expectedBytes)
            throws IOException {
        if (bufferWithIdentities.isEmpty()) {
            return;
        }

        ByteBuffer[] bufferWithHeaders = generateBufferWithHeaders(bufferWithIdentities);

        BufferReaderWriterUtil.writeBuffers(dataFileChannel, expectedBytes, bufferWithHeaders);
        totalBytesWritten += expectedBytes;
    }

    private void setBufferWithHeader(Buffer buffer, ByteBuffer[] bufferWithHeaders, int index) {
        ByteBuffer header = BufferReaderWriterUtil.allocatedHeaderBuffer();
        BufferReaderWriterUtil.setByteChannelBufferHeader(buffer, header);

        bufferWithHeaders[index] = header;
        bufferWithHeaders[index + 1] = buffer.getNioBufferReadable();
    }

    /**
     * Close this {@link DiskCacheBufferSpiller} when resultPartition is closed. It means spiller
     * will no longer accept new spilling operation.
     *
     * <p>This method only called by main task thread.
     */
    @Override
    public void close() {}

    /**
     * Release this {@link DiskCacheBufferSpiller} when resultPartition is released. It means
     * spiller will wait for all previous spilling operation done blocking and close the file
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
    }

    @VisibleForTesting
    @Override
    public org.apache.flink.core.fs.Path getBaseSubpartitionPath() {
        return null;
    }
}