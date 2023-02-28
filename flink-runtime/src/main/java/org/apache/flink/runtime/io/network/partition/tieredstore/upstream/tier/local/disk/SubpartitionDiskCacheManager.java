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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.Buffer.DataType;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.BufferContext;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.BufferIndexAndChannel;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.BufferWithIdentity;
import org.apache.flink.util.function.SupplierWithException;
import org.apache.flink.util.function.ThrowingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** This class is responsible for managing the data in a single subpartition. */
public class SubpartitionDiskCacheManager {

    private static final Logger LOG = LoggerFactory.getLogger(SubpartitionDiskCacheManager.class);

    private final int targetChannel;

    private final int bufferSize;

    private final DiskCacheManagerOperation diskCacheManagerOperation;

    // Not guarded by lock because it is expected only accessed from task's main thread.
    private final Queue<BufferBuilder> unfinishedBuffers = new LinkedList<>();

    // Not guarded by lock because it is expected only accessed from task's main thread.
    private int finishedBufferIndex;

    @GuardedBy("subpartitionLock")
    private final Deque<BufferContext> allBuffers = new LinkedList<>();

    @GuardedBy("subpartitionLock")
    private final Map<Integer, BufferContext> bufferIndexToContexts = new HashMap<>();

    @GuardedBy("subpartitionLock")
    private final Set<Integer> lastBufferIndexOfSegments;

    /** DO NOT USE DIRECTLY. Use {@link #runWithLock} or {@link #callWithLock} instead. */
    private final ReentrantLock subpartitionLock = new ReentrantLock();

    @Nullable private final BufferCompressor bufferCompressor;

    @Nullable private OutputMetrics outputMetrics;

    public SubpartitionDiskCacheManager(
            int targetChannel,
            int bufferSize,
            @Nullable BufferCompressor bufferCompressor,
            DiskCacheManagerOperation diskCacheManagerOperation) {
        this.targetChannel = targetChannel;
        this.bufferSize = bufferSize;
        this.diskCacheManagerOperation = diskCacheManagerOperation;
        this.bufferCompressor = bufferCompressor;
        this.lastBufferIndexOfSegments = new HashSet<>();
    }

    // ------------------------------------------------------------------------
    //  Called by MemoryDataManager
    // ------------------------------------------------------------------------

    public void append(ByteBuffer record, DataType dataType, boolean isLastRecordInSegment)
            throws InterruptedException {
        if (dataType.isEvent()) {
            writeEvent(record, dataType, isLastRecordInSegment);
        } else {
            writeRecord(record, dataType, isLastRecordInSegment);
        }
    }

    // Note that: callWithLock ensure that code block guarded by resultPartitionReadLock and
    // subpartitionLock.
    public Deque<BufferIndexAndChannel> getBuffersSatisfyStatus(
            DiskCacheManagerOperation.SpillStatus spillStatus,
            DiskCacheManagerOperation.ConsumeStatusWithId consumeStatusWithId) {
        return callWithLock(
                () -> {
                    // TODO return iterator to avoid completely traversing the queue for each call.
                    Deque<BufferIndexAndChannel> targetBuffers = new ArrayDeque<>();
                    // traverse buffers in order.
                    allBuffers.forEach(
                            (bufferContext -> {
                                if (isBufferSatisfyStatus(
                                        bufferContext, spillStatus, consumeStatusWithId)) {
                                    targetBuffers.add(bufferContext.getBufferIndexAndChannel());
                                }
                            }));
                    return targetBuffers;
                });
    }

    /**
     * Spill this subpartition's buffers in a decision.
     *
     * @param toSpill All buffers that need to be spilled belong to this subpartition in a decision.
     * @param spillDoneFuture completed when spill is finished.
     * @return {@link BufferWithIdentity}s about these spill buffers.
     */
    @SuppressWarnings("FieldAccessNotGuarded")
    // Note that: callWithLock ensure that code block guarded by resultPartitionReadLock and
    // subpartitionLock.
    public List<BufferWithIdentity> spillSubpartitionBuffers(
            List<BufferIndexAndChannel> toSpill, CompletableFuture<Void> spillDoneFuture) {
        return callWithLock(
                () ->
                        toSpill.stream()
                                .map(
                                        indexAndChannel -> {
                                            int bufferIndex = indexAndChannel.getBufferIndex();
                                            return startSpillingBuffer(bufferIndex, spillDoneFuture)
                                                    .map(
                                                            (context) ->
                                                                    new BufferWithIdentity(
                                                                            context.getBuffer(),
                                                                            bufferIndex,
                                                                            targetChannel));
                                        })
                                .filter(Optional::isPresent)
                                .map(Optional::get)
                                .collect(Collectors.toList()));
    }

    /**
     * Release this subpartition's buffers in a decision.
     *
     * @param toRelease All buffers that need to be released belong to this subpartition in a
     *     decision.
     */
    @SuppressWarnings("FieldAccessNotGuarded")
    // Note that: runWithLock ensure that code block guarded by resultPartitionReadLock and
    // subpartitionLock.
    public void releaseSubpartitionBuffers(List<BufferIndexAndChannel> toRelease) {
        runWithLock(
                () ->
                        toRelease.forEach(
                                (indexAndChannel) -> {
                                    int bufferIndex = indexAndChannel.getBufferIndex();
                                    BufferContext bufferContext =
                                            bufferIndexToContexts.get(bufferIndex);
                                    if (bufferContext != null) {
                                        checkAndMarkBufferReadable(bufferContext);
                                        releaseBuffer(bufferIndex);
                                    }
                                }));
    }

    public void setOutputMetrics(OutputMetrics outputMetrics) {
        this.outputMetrics = checkNotNull(outputMetrics);
    }

    Set<Integer> getLastBufferIndexOfSegments() {
        return lastBufferIndexOfSegments;
    }

    public void release() {
        lastBufferIndexOfSegments.clear();
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    private void writeEvent(ByteBuffer event, DataType dataType, boolean isLastRecordInSegment) {
        checkArgument(dataType.isEvent());

        // each Event must take an exclusive buffer
        finishCurrentWritingBufferIfNotEmpty();

        // store Events in adhoc heap segments, for network memory efficiency
        MemorySegment data = MemorySegmentFactory.wrap(event.array());
        Buffer buffer =
                new NetworkBuffer(data, FreeingBufferRecycler.INSTANCE, dataType, data.size());

        BufferContext bufferContext =
                new BufferContext(
                        buffer, finishedBufferIndex, targetChannel, isLastRecordInSegment);
        LOG.debug("%%% add a finished Event");
        addFinishedBuffer(bufferContext);
    }

    private void writeRecord(ByteBuffer record, DataType dataType, boolean isLastRecordInSegment)
            throws InterruptedException {
        checkArgument(!dataType.isEvent());

        ensureCapacityForRecord(record);

        writeRecord(record, isLastRecordInSegment);
    }

    private void ensureCapacityForRecord(ByteBuffer record) throws InterruptedException {
        final int numRecordBytes = record.remaining();
        int availableBytes =
                Optional.ofNullable(unfinishedBuffers.peek())
                        .map(
                                currentWritingBuffer ->
                                        currentWritingBuffer.getWritableBytes()
                                                + bufferSize * (unfinishedBuffers.size() - 1))
                        .orElse(0);

        while (availableBytes < numRecordBytes) {
            // request unfinished buffer.
            BufferBuilder bufferBuilder = diskCacheManagerOperation.requestBufferFromPool();
            unfinishedBuffers.add(bufferBuilder);
            availableBytes += bufferSize;
        }
    }

    private void writeRecord(ByteBuffer record, boolean isLastRecordInSegment) {
        while (record.hasRemaining()) {
            BufferBuilder currentWritingBuffer =
                    checkNotNull(
                            unfinishedBuffers.peek(), "Expect enough capacity for the record.");
            currentWritingBuffer.append(record);
            LOG.debug("%%% add a finished record1");
            if (currentWritingBuffer.isFull() && record.hasRemaining()) {
                LOG.debug("%%% add a finished record2");
                finishCurrentWritingBuffer(false);
            } else if (currentWritingBuffer.isFull() && !record.hasRemaining()) {
                LOG.debug("%%% add a finished record3");
                finishCurrentWritingBuffer(isLastRecordInSegment);
            } else if (!currentWritingBuffer.isFull() && !record.hasRemaining()) {
                LOG.debug("%%% add a finished record4");
                if (isLastRecordInSegment) {
                    LOG.debug("%%% add a finished record5");
                    finishCurrentWritingBuffer(true);
                }
            }
        }
    }

    private void finishCurrentWritingBufferIfNotEmpty() {
        BufferBuilder currentWritingBuffer = unfinishedBuffers.peek();
        if (currentWritingBuffer == null || currentWritingBuffer.getWritableBytes() == bufferSize) {
            return;
        }

        finishCurrentWritingBuffer(false);
    }

    private void finishCurrentWritingBuffer(boolean isLastBufferInSegment) {
        BufferBuilder currentWritingBuffer = unfinishedBuffers.poll();
        LOG.debug("%%% add a finished record6");
        if (currentWritingBuffer == null) {
            return;
        }
        LOG.debug("%%% add a finished record7");
        currentWritingBuffer.finish();
        BufferConsumer bufferConsumer = currentWritingBuffer.createBufferConsumerFromBeginning();
        Buffer buffer = bufferConsumer.build();
        currentWritingBuffer.close();
        bufferConsumer.close();
        BufferContext bufferContext =
                new BufferContext(
                        compressBuffersIfPossible(buffer),
                        finishedBufferIndex,
                        targetChannel,
                        isLastBufferInSegment);
        addFinishedBuffer(bufferContext);
    }

    private Buffer compressBuffersIfPossible(Buffer buffer) {
        if (!canBeCompressed(buffer)) {
            return buffer;
        }
        return checkNotNull(bufferCompressor).compressToOriginalBuffer(buffer);
    }

    /**
     * Whether the buffer can be compressed or not. Note that event is not compressed because it is
     * usually small and the size can become even larger after compression.
     */
    private boolean canBeCompressed(Buffer buffer) {
        return bufferCompressor != null && buffer.isBuffer() && buffer.readableBytes() > 0;
    }

    @SuppressWarnings("FieldAccessNotGuarded")
    // Note that: callWithLock ensure that code block guarded by resultPartitionReadLock and
    // subpartitionLock.
    private void addFinishedBuffer(BufferContext bufferContext) {
        runWithLock(
                () -> {
                    LOG.debug("%%% add a finished Buffer");
                    finishedBufferIndex++;
                    allBuffers.add(bufferContext);
                    bufferIndexToContexts.put(
                            bufferContext.getBufferIndexAndChannel().getBufferIndex(),
                            bufferContext);
                    updateStatistics(bufferContext.getBuffer());
                    if (bufferContext.isLastBufferInSegment()) {
                        lastBufferIndexOfSegments.add(finishedBufferIndex - 1);
                    }
                });
    }

    /**
     * Remove all released buffer from head of queue until buffer queue is empty or meet un-released
     * buffer.
     */
    @GuardedBy("subpartitionLock")
    private void trimHeadingReleasedBuffers(Deque<BufferContext> bufferQueue) {
        while (!bufferQueue.isEmpty() && bufferQueue.peekFirst().isReleased()) {
            LOG.debug("%%% I'm trying to remove Buffer.. ");
            bufferQueue.removeFirst();
        }
    }

    @GuardedBy("subpartitionLock")
    private void releaseBuffer(int bufferIndex) {
        BufferContext bufferContext = bufferIndexToContexts.remove(bufferIndex);
        if (bufferContext == null) {
            return;
        }
        bufferContext.release();
        // remove released buffers from head lazy.
        LOG.debug("%%% I'm trying to release Buffer.. ");
        trimHeadingReleasedBuffers(allBuffers);
    }

    @GuardedBy("subpartitionLock")
    private Optional<BufferContext> startSpillingBuffer(
            int bufferIndex, CompletableFuture<Void> spillFuture) {
        BufferContext bufferContext = bufferIndexToContexts.get(bufferIndex);
        if (bufferContext == null) {
            return Optional.empty();
        }
        return bufferContext.startSpilling(spillFuture)
                ? Optional.of(bufferContext)
                : Optional.empty();
    }

    @GuardedBy("subpartitionLock")
    private void checkAndMarkBufferReadable(BufferContext bufferContext) {
        // only spill buffer needs to be marked as released.
        if (isBufferSatisfyStatus(
                bufferContext,
                DiskCacheManagerOperation.SpillStatus.SPILL,
                DiskCacheManagerOperation.ConsumeStatusWithId.ALL_ANY)) {
            bufferContext
                    .getSpilledFuture()
                    .orElseThrow(
                            () ->
                                    new IllegalStateException(
                                            "Buffer in spill status should already set spilled future."))
                    .thenRun(
                            () -> {
                                BufferIndexAndChannel bufferIndexAndChannel =
                                        bufferContext.getBufferIndexAndChannel();
                                diskCacheManagerOperation.markBufferReleasedFromFile(
                                        bufferIndexAndChannel.getChannel(),
                                        bufferIndexAndChannel.getBufferIndex());
                            });
        }
    }

    @GuardedBy("subpartitionLock")
    private boolean isBufferSatisfyStatus(
            BufferContext bufferContext,
            DiskCacheManagerOperation.SpillStatus spillStatus,
            DiskCacheManagerOperation.ConsumeStatusWithId consumeStatusWithId) {
        // released buffer is not needed.
        if (bufferContext.isReleased()) {
            return false;
        }
        boolean match = true;
        switch (spillStatus) {
            case NOT_SPILL:
                match = !bufferContext.isSpillStarted();
                break;
            case SPILL:
                match = bufferContext.isSpillStarted();
                break;
        }
        switch (consumeStatusWithId.status) {
            case NOT_CONSUMED:
                match &= !bufferContext.isConsumed(consumeStatusWithId.tierReaderViewId);
                break;
            case CONSUMED:
                match &= bufferContext.isConsumed(consumeStatusWithId.tierReaderViewId);
                break;
        }
        return match;
    }

    private void updateStatistics(Buffer buffer) {
        checkNotNull(outputMetrics).getNumBuffersOut().inc();
        checkNotNull(outputMetrics).getNumBytesOut().inc(buffer.readableBytes());
    }

    private <E extends Exception> void runWithLock(ThrowingRunnable<E> runnable) throws E {
        try {
            subpartitionLock.lock();
            runnable.run();
        } finally {
            subpartitionLock.unlock();
        }
    }

    private <R, E extends Exception> R callWithLock(SupplierWithException<R, E> callable) throws E {
        try {
            subpartitionLock.lock();
            return callable.get();
        } finally {
            subpartitionLock.unlock();
        }
    }
}