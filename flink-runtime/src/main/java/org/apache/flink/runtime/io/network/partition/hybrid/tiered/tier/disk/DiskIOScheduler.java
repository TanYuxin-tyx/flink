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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.disk;

import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.PartitionFileReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyPayload;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyServiceProducer;
import org.apache.flink.util.FatalExitExceptionHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The {@link DiskIOScheduler} is a scheduler that controls the reading of data from shuffle files.
 * It ensures the correct order of buffers in each subpartition during file reading. The scheduler
 * implements the {@link NettyServiceProducer} interface to send the buffers to the Netty server
 * through the {@link NettyConnectionWriter}.
 */
public class DiskIOScheduler implements Runnable, BufferRecycler, NettyServiceProducer {

    private static final Logger LOG = LoggerFactory.getLogger(DiskIOScheduler.class);

    private final ByteBuffer reusedHeaderBuffer = BufferReaderWriterUtil.allocatedHeaderBuffer();

    private final Object lock = new Object();

    /** The partition id. */
    private final TieredStoragePartitionId partitionId;

    /** The executor is responsible for scheduling the disk read process. */
    private final ScheduledExecutorService ioExecutor;

    /**
     * The buffer pool is specifically designed for reading from disk and shared in the TaskManager.
     */
    private final BatchShuffleReadBufferPool bufferPool;

    /**
     * The maximum number of buffers that can be allocated and still not recycled for a
     * subpartition, which ensures that each subpartition can be consumed evenly.
     */
    private final int maxBufferReadAhead;

    /**
     * The maximum number of buffers that can be allocated and still not recycled by a single {@link
     * DiskIOScheduler} for all subpartitions. This ensures that different {@link DiskIOScheduler}s
     * in the TaskManager can evenly use the buffer pool.
     */
    private final int maxRequestedBuffers;

    /**
     * The maximum time to wait when requesting read buffers from the buffer pool before throwing an
     * exception.
     */
    private final Duration bufferRequestTimeout;

    /**
     * Retrieve the segment id if the buffer index represents the first buffer. The first integer is
     * the id of subpartition, and the second integer is buffer index and the value is segment id.
     */
    private final BiFunction<Integer, Integer, Integer> firstBufferIndexInSegmentRetriever;

    private final PartitionFileReader partitionFileReader;

    @GuardedBy("lock")
    private final Map<NettyConnectionId, ScheduledSubpartitionReader> allScheduledReaders =
            new HashMap<>();

    @GuardedBy("lock")
    private boolean isRunning;

    @GuardedBy("lock")
    private int numRequestedBuffers;

    @GuardedBy("lock")
    private boolean isReleased;

    private boolean shouldPrint = true;

    private final String taskName;

    private final boolean isBroadcast;

    public DiskIOScheduler(
            boolean isBroadcast,
            String taskName,
            TieredStoragePartitionId partitionId,
            BatchShuffleReadBufferPool bufferPool,
            ScheduledExecutorService ioExecutor,
            int maxRequestedBuffers,
            Duration bufferRequestTimeout,
            int maxBufferReadAhead,
            BiFunction<Integer, Integer, Integer> firstBufferIndexInSegmentRetriever,
            PartitionFileReader partitionFileReader) {
        this.taskName = taskName;
        this.partitionId = partitionId;
        this.bufferPool = checkNotNull(bufferPool);
        this.ioExecutor = checkNotNull(ioExecutor);
        this.maxRequestedBuffers = maxRequestedBuffers;
        this.bufferRequestTimeout = checkNotNull(bufferRequestTimeout);
        this.maxBufferReadAhead = maxBufferReadAhead;
        this.firstBufferIndexInSegmentRetriever = firstBufferIndexInSegmentRetriever;
        this.partitionFileReader = partitionFileReader;
        this.isBroadcast = isBroadcast;
        bufferPool.registerRequester(this);
    }

    @Override
    public synchronized void run() {
        int numBuffersRead = readBuffersFromFile();
        synchronized (lock) {
            numRequestedBuffers += numBuffersRead;
            isRunning = false;
        }
        if (numBuffersRead == 0) {
            ioExecutor.schedule(this::triggerScheduling, 5, TimeUnit.MILLISECONDS);
        } else {
            triggerScheduling();
        }
    }

    @Override
    public void connectionEstablished(
            TieredStorageSubpartitionId subpartitionId,
            NettyConnectionWriter nettyConnectionWriter) {
        synchronized (lock) {
            checkState(!isReleased, "DiskIOScheduler is already released.");
            ScheduledSubpartitionReader scheduledSubpartitionReader;
            if (shouldPrint && isBroadcast) {
                scheduledSubpartitionReader =
                        new ScheduledSubpartitionReader(
                                subpartitionId, nettyConnectionWriter, reusedHeaderBuffer, true);
                shouldPrint = false;
            } else {
                scheduledSubpartitionReader =
                        new ScheduledSubpartitionReader(
                                subpartitionId, nettyConnectionWriter, reusedHeaderBuffer, false);
            }
            allScheduledReaders.put(
                    nettyConnectionWriter.getNettyConnectionId(), scheduledSubpartitionReader);
            triggerScheduling();
        }
    }

    @Override
    public void connectionBroken(NettyConnectionId id) {
        synchronized (lock) {
            allScheduledReaders.remove(id);
        }
    }

    @Override
    public void recycle(MemorySegment segment) {
        synchronized (lock) {
            bufferPool.recycle(segment);
            --numRequestedBuffers;
            triggerScheduling();
        }
    }

    public void release() {
        synchronized (lock) {
            if (isReleased) {
                return;
            }
            isReleased = true;
            allScheduledReaders.clear();
            partitionFileReader.release();
            bufferPool.unregisterRequester(this);
        }
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    private int readBuffersFromFile() {
        List<ScheduledSubpartitionReader> scheduledReaders = sortScheduledReaders();
        if (scheduledReaders.isEmpty()) {
            return 0;
        }
        Queue<MemorySegment> buffers;
        try {
            buffers = allocateBuffers();
        } catch (Exception exception) {
            failScheduledReaders(scheduledReaders, exception);
            LOG.error("Failed to request buffers for data reading.", exception);
            return 0;
        }
        LOG.error("###" + taskName + " num requested buffers: " + buffers.size());

        int numBuffersAllocated = buffers.size();
        if (numBuffersAllocated <= 0) {
            return 0;
        }

        for (ScheduledSubpartitionReader scheduledReader : scheduledReaders) {
            // if (buffers.isEmpty()) {
            //    break;
            // }
            try {
                scheduledReader.loadDiskDataToBuffers(buffers, this);
            } catch (Exception throwable) {
                failScheduledReaders(Collections.singletonList(scheduledReader), throwable);
                LOG.debug("Failed to read shuffle data.", throwable);
            }
        }
        int numBuffersRead = numBuffersAllocated - buffers.size();
        LOG.error(
                "###"
                        + taskName
                        + " num released buffers: "
                        + buffers.size()
                        + " numRead: "
                        + numBuffersRead);
        releaseBuffers(buffers);
        return numBuffersRead;
    }

    private List<ScheduledSubpartitionReader> sortScheduledReaders() {
        List<ScheduledSubpartitionReader> scheduledReaders;
        synchronized (lock) {
            if (isReleased) {
                return new ArrayList<>();
            }
            scheduledReaders = new ArrayList<>(allScheduledReaders.values());
        }
        for (ScheduledSubpartitionReader reader : scheduledReaders) {
            reader.prepareForScheduling();
        }
        Collections.sort(scheduledReaders);
        return scheduledReaders;
    }

    private Queue<MemorySegment> allocateBuffers() throws Exception {
        long timeoutTime = getBufferRequestTimeoutTime();
        do {
            List<MemorySegment> buffers = bufferPool.requestBuffers();
            if (!buffers.isEmpty()) {
                return new ArrayDeque<>(buffers);
            }
            synchronized (lock) {
                if (isReleased) {
                    return new ArrayDeque<>();
                }
            }
        } while (System.currentTimeMillis() < timeoutTime
                || System.currentTimeMillis() < (timeoutTime = getBufferRequestTimeoutTime()));
        throw new TimeoutException(
                String.format(
                        "Buffer request timeout, this means there is a fierce contention of"
                                + " the batch shuffle read memory, please increase '%s'.",
                        TaskManagerOptions.NETWORK_BATCH_SHUFFLE_READ_MEMORY.key()));
    }

    private void failScheduledReaders(
            List<ScheduledSubpartitionReader> scheduledReaders, Throwable failureCause) {
        for (ScheduledSubpartitionReader scheduledReader : scheduledReaders) {
            synchronized (lock) {
                allScheduledReaders.remove(scheduledReader.getId());
            }
            scheduledReader.failReader(failureCause);
        }
    }

    private void releaseBuffers(Queue<MemorySegment> buffers) {
        if (!buffers.isEmpty()) {
            try {
                bufferPool.recycle(buffers);
                buffers.clear();
            } catch (Throwable throwable) {
                // this should never happen so just trigger fatal error
                FatalExitExceptionHandler.INSTANCE.uncaughtException(
                        Thread.currentThread(), throwable);
            }
        }
    }

    void triggerScheduling() {
        synchronized (lock) {
            if (!isRunning
                    && !allScheduledReaders.isEmpty()
                    && numRequestedBuffers + bufferPool.getNumBuffersPerRequest()
                            <= maxRequestedBuffers
                    && numRequestedBuffers < bufferPool.getAverageBuffersPerRequester()) {
                isRunning = true;
                ioExecutor.execute(
                        () -> {
                            try {
                                run();
                            } catch (Throwable throwable) {
                                LOG.error("Failed to read data.", throwable);
                                // handle un-expected exception as unhandledExceptionHandler is not
                                // worked for ScheduledExecutorService.
                                FatalExitExceptionHandler.INSTANCE.uncaughtException(
                                        Thread.currentThread(), throwable);
                            }
                        });
            }
        }
    }

    private long getBufferRequestTimeoutTime() {
        return bufferPool.getLastBufferOperationTimestamp() + bufferRequestTimeout.toMillis();
    }

    /**
     * The {@link ScheduledSubpartitionReader} is responsible for reading a subpartition from disk,
     * and is scheduled by the {@link DiskIOScheduler}.
     */
    private class ScheduledSubpartitionReader implements Comparable<ScheduledSubpartitionReader> {

        private final TieredStorageSubpartitionId subpartitionId;

        private final NettyConnectionWriter nettyConnectionWriter;

        private final ByteBuffer reusedHeaderBuffer;

        private int nextSegmentId = -1;

        private int nextBufferIndex;

        private long priority;

        private boolean isFailed;

        private PartitionFileReader.PartialBuffer partialBuffer;

        private final boolean shouldPrintLog;

        private ScheduledSubpartitionReader(
                TieredStorageSubpartitionId subpartitionId,
                NettyConnectionWriter nettyConnectionWriter,
                ByteBuffer reusedHeaderBuffer,
                boolean shouldPrintLog) {
            this.shouldPrintLog = shouldPrintLog;
            if (shouldPrintLog) {
                LOG.error("###" + taskName + " Start reading..");
            }
            this.subpartitionId = subpartitionId;
            this.nettyConnectionWriter = nettyConnectionWriter;
            this.reusedHeaderBuffer = reusedHeaderBuffer;
        }

        private void loadDiskDataToBuffers(Queue<MemorySegment> buffers, BufferRecycler recycler)
                throws IOException {

            if (isFailed) {
                throw new IOException(
                        "The scheduled subpartition reader for "
                                + subpartitionId
                                + " has already been failed.");
            }
            if (shouldPrintLog) {
                LOG.error("###" + taskName + " Availability buffer number " + buffers.size());
            }

            int numReadBuffers = 0;
            while (!buffers.isEmpty()
                    && numReadBuffers < maxBufferReadAhead
                    && nextSegmentId >= 0) {
                if (shouldPrintLog) {
                    LOG.error("###" + taskName + " Start Poll");
                }
                MemorySegment memorySegment = buffers.poll();
                numReadBuffers++;
                List<Buffer> readBuffers;
                try {
                    if ((readBuffers =
                                    partitionFileReader.readBuffer(
                                            shouldPrintLog,
                                            taskName,
                                            partitionId,
                                            subpartitionId,
                                            nextSegmentId,
                                            nextBufferIndex,
                                            memorySegment,
                                            recycler,
                                            reusedHeaderBuffer,
                                            partialBuffer))
                            == null) {
                        buffers.add(memorySegment);
                        break;
                    }
                } catch (Throwable throwable) {
                    buffers.add(memorySegment);
                    throw throwable;
                }

                partialBuffer = null;
                for (int i = 0; i < readBuffers.size(); i++) {
                    Buffer readBuffer = readBuffers.get(i);
                    if (shouldPrintLog) {
                        LOG.error(
                                "###"
                                        + taskName
                                        + " poll buffer index "
                                        + nextBufferIndex
                                        + " buffer size: "
                                        + readBuffer.readableBytes());
                    }

                    if (i == readBuffers.size() - 1
                            && readBuffer instanceof PartitionFileReader.PartialBuffer) {
                        partialBuffer = (PartitionFileReader.PartialBuffer) readBuffer;
                        continue;
                    }

                    writeToNettyConnectionWriter(
                            NettyPayload.newBuffer(
                                    readBuffer,
                                    nextBufferIndex++,
                                    subpartitionId.getSubpartitionId()));
                    if (readBuffer.getDataType() == Buffer.DataType.END_OF_SEGMENT) {
                        nextSegmentId = -1;
                        updateSegmentId();
                    }
                }
            }
            if (reusedHeaderBuffer.position() > 0) {
                reusedHeaderBuffer.clear();
            }
            if (partialBuffer != null) {
                partialBuffer.recycleBuffer();
                partialBuffer = null;
            }
        }

        @Override
        public int compareTo(ScheduledSubpartitionReader reader) {
            checkArgument(reader != null);
            return Long.compare(getPriority(), reader.getPriority());
        }

        private void prepareForScheduling() {
            if (nextSegmentId < 0) {
                updateSegmentId();
            }
            priority =
                    nextSegmentId < 0
                            ? Long.MAX_VALUE
                            : partialBuffer != null
                                    ? partialBuffer.getFileOffset()
                                    : partitionFileReader.getPriority(
                                            partitionId,
                                            subpartitionId,
                                            nextSegmentId,
                                            nextBufferIndex);
        }

        private void writeToNettyConnectionWriter(NettyPayload nettyPayload) {
            nettyConnectionWriter.writeBuffer(nettyPayload);
            if (shouldPrintLog) {
                LOG.info(
                        "###"
                                + taskName
                                + " netty payload queue size: "
                                + nettyConnectionWriter.numQueuedBuffers());
            }
            if (nettyConnectionWriter.numQueuedBuffers() <= 1) {
                notifyAvailable();
            }
        }

        private long getPriority() {
            return priority;
        }

        private void notifyAvailable() {
            nettyConnectionWriter.notifyAvailable();
        }

        private void failReader(Throwable failureCause) {
            if (isFailed) {
                return;
            }
            isFailed = true;
            nettyConnectionWriter.close(failureCause);
            nettyConnectionWriter.notifyAvailable();
        }

        private void updateSegmentId() {
            Integer segmentId =
                    firstBufferIndexInSegmentRetriever.apply(
                            subpartitionId.getSubpartitionId(), nextBufferIndex);
            if (segmentId != null) {
                nextSegmentId = segmentId;
                writeToNettyConnectionWriter(NettyPayload.newSegment(segmentId));
            }
        }

        private NettyConnectionId getId() {
            return nettyConnectionWriter.getNettyConnectionId();
        }
    }
}
