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
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.PartitionFileIndex;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.PartitionFileReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyService;
import org.apache.flink.util.FatalExitExceptionHandler;

import org.apache.flink.shaded.guava30.com.google.common.collect.HashBiMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** The default implementation of {@link DiskIOScheduler}. */
public class DiskIOSchedulerImpl implements DiskIOScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(DiskIOSchedulerImpl.class);

    private final Object lock = new Object();

    private final TieredStoragePartitionId partitionId;

    private final ScheduledExecutorService ioExecutor;

    private final Duration bufferRequestTimeout;

    private final BatchShuffleReadBufferPool bufferPool;

    private final int maxBufferReadAhead;

    private final int maxRequestedBuffers;

    private final TieredStorageNettyService nettyService;

    private final List<Map<Integer, Integer>> segmentIdRecorder;

    private final PartitionFileReader partitionFileReader;

    private final PartitionFileIndex dataIndex;

    @GuardedBy("lock")
    private final HashBiMap<NettyConnectionId, ScheduledSubpartitionReader> allScheduledReaders =
            HashBiMap.create();

    @GuardedBy("lock")
    private volatile boolean isRunning;

    @GuardedBy("lock")
    private volatile int numRequestedBuffers;

    @GuardedBy("lock")
    private volatile boolean isReleased;

    public DiskIOSchedulerImpl(
            TieredStoragePartitionId partitionId,
            BatchShuffleReadBufferPool bufferPool,
            ScheduledExecutorService ioExecutor,
            int maxRequestedBuffers,
            Duration bufferRequestTimeout,
            int maxBufferReadAhead,
            TieredStorageNettyService nettyService,
            List<Map<Integer, Integer>> segmentIdRecorder,
            PartitionFileReader partitionFileReader,
            PartitionFileIndex dataIndex) {
        this.partitionId = partitionId;
        this.bufferPool = checkNotNull(bufferPool);
        this.ioExecutor = checkNotNull(ioExecutor);
        this.maxRequestedBuffers = maxRequestedBuffers;
        this.bufferRequestTimeout = checkNotNull(bufferRequestTimeout);
        this.maxBufferReadAhead = maxBufferReadAhead;
        this.nettyService = nettyService;
        this.segmentIdRecorder = segmentIdRecorder;
        this.partitionFileReader = partitionFileReader;
        this.dataIndex = dataIndex;
        bufferPool.registerRequester(this);
    }

    @Override
    public synchronized void run() {
        int numBuffersRead = readBuffersFromFile();
        synchronized (lock) {
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
            checkState(!isReleased, "ProducerMergePartitionFileReader is already released.");
            ScheduledSubpartitionReaderImpl scheduledSubpartitionReaderImpl =
                    new ScheduledSubpartitionReaderImpl(
                            partitionId,
                            subpartitionId,
                            maxBufferReadAhead,
                            nettyConnectionWriter,
                            nettyService,
                            segmentIdRecorder.get(subpartitionId.getSubpartitionId()),
                            partitionFileReader,
                            dataIndex);
            allScheduledReaders.put(
                    nettyConnectionWriter.getNettyConnectionId(), scheduledSubpartitionReaderImpl);
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
            segmentIdRecorder.clear();
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

        int numBuffersAllocated = buffers.size();
        if (numBuffersAllocated <= 0) {
            return 0;
        }

        int startIndex = 0;
        while (startIndex < scheduledReaders.size() && !buffers.isEmpty()) {
            ScheduledSubpartitionReader scheduledReader = scheduledReaders.get(startIndex);
            startIndex++;
            try {
                scheduledReader.loadDiskDataToBuffers(buffers, this);
            } catch (IOException throwable) {
                failScheduledReaders(Collections.singletonList(scheduledReader), throwable);
                LOG.debug("Failed to read shuffle data.", throwable);
            }
        }
        int numBuffersRead = numBuffersAllocated - buffers.size();
        releaseBuffers(buffers);
        synchronized (lock) {
            numRequestedBuffers += numBuffersRead;
        }
        return numBuffersRead;
    }

    private List<ScheduledSubpartitionReader> sortScheduledReaders() {
        synchronized (lock) {
            if (isReleased) {
                return new ArrayList<>();
            }
            List<ScheduledSubpartitionReader> scheduledReaders =
                    new ArrayList<>(allScheduledReaders.values());
            Collections.sort(scheduledReaders);
            return scheduledReaders;
        }
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
                allScheduledReaders.remove(allScheduledReaders.inverse().get(scheduledReader));
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

    private void triggerScheduling() {
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
}
