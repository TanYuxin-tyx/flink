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

package org.apache.flink.runtime.io.network.partition.store.history.memory;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ChannelStateHolder;
import org.apache.flink.runtime.io.network.partition.CheckpointedResultSubpartition;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.store.TieredStoreMode;
import org.apache.flink.runtime.io.network.partition.store.TieredStoreResultPartition;
import org.apache.flink.runtime.io.network.partition.store.common.BufferIndexOfLastRecordInSegmentTracker;
import org.apache.flink.runtime.io.network.partition.store.common.BufferPoolHelper;
import org.apache.flink.runtime.io.network.partition.store.common.SingleTierDataGate;
import org.apache.flink.runtime.io.network.partition.store.common.SingleTierReader;
import org.apache.flink.runtime.io.network.partition.store.common.SingleTierWriter;
import org.apache.flink.runtime.io.network.partition.store.common.SubpartitionSegmentIndexTracker;
import org.apache.flink.runtime.io.network.partition.store.tier.local.file.OutputMetrics;
import org.apache.flink.runtime.metrics.TimerGauge;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkElementIndex;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** The DataManager of Memory. */
public class LocalMemoryDataManager
        implements SingleTierDataGate, SingleTierWriter, LocalMemoryDataManagerOperations {

    private static final Logger LOG = LoggerFactory.getLogger(LocalMemoryDataManager.class);

    //////////////////////////// From PipelinedResultPartition /////////////////////////////////////

    private static final int PIPELINED_RESULT_PARTITION_ITSELF = -42;

    /**
     * The lock that guard operations which can be asynchronously propagated from the networks
     * threads.
     */
    private final Object lock = new Object();

    /**
     * A flag for each subpartition indicating whether the downstream task has processed all the
     * user records.
     */
    @GuardedBy("lock")
    private final boolean[] allRecordsProcessedSubpartitions;

    /**
     * The total number of subpartitions whose user records have not been fully processed by the
     * downstream tasks yet.
     */
    @GuardedBy("lock")
    private int numNotAllRecordsProcessedSubpartitions;

    /**
     * The future represents whether all the records has been processed by all the downstream tasks.
     */
    @GuardedBy("lock")
    private final CompletableFuture<Void> allRecordsProcessedFuture = new CompletableFuture<>();

    /**
     * A flag for each subpartition indicating whether it was already consumed or not, to make
     * releases idempotent.
     */
    @GuardedBy("lock")
    private final boolean[] consumedSubpartitions;

    /**
     * The total number of references to subpartitions of this result. The result partition can be
     * safely released, iff the reference count is zero. Every subpartition is an user of the result
     * as well the {@link LocalMemoryDataManager} is a user itself, as it's writing to those
     * results. Even if all consumers are released, partition can not be released until writer
     * releases the partition as well.
     */
    @GuardedBy("lock")
    private int numberOfUsers;

    ////////////////////////// From BufferWritingResultPartition ///////////////////////////////////

    /** The subpartitions of this partition. At least one. */
    protected final ResultSubpartition[] subpartitions;

    /**
     * For non-broadcast mode, each subpartition maintains a separate BufferBuilder which might be
     * null.
     */
    private final BufferBuilder[] unicastBufferBuilders;

    /** For broadcast mode, a single BufferBuilder is shared by all subpartitions. */
    private BufferBuilder broadcastBufferBuilder;

    private TimerGauge hardBackPressuredTimeMsPerSecond = new TimerGauge();

    private long totalWrittenBytes;

    ////////////////////////// From TieredStoreResultPartition /////////////////////////////////////

    private final TieredStoreResultPartition tieredStoreResultPartition;

    private final int numSubpartitions;

    private int numBytesInASegment = 50 * 32 * 1024;

    private final int bufferNumberInSegment = numBytesInASegment / 32 / 1024;

    private final BufferPoolHelper bufferPoolHelper;

    private final SubpartitionSegmentIndexTracker segmentIndexTracker;

    private final BufferIndexOfLastRecordInSegmentTracker bufferIndexOfLastRecordInSegment;

    private volatile boolean isReleased;

    private volatile boolean isClosed;

    private Counter numBuffersOut;

    private Counter numBytesOut;

    private Counter numBytesProduced;

    private final Boolean isBroadcastOnly;

    private final int[] finishedBufferIndex;

    private final int configuredNetworkBuffersPerChannel;

    public LocalMemoryDataManager(
            ResultSubpartition[] subpartitions,
            int numSubpartitions,
            TieredStoreResultPartition tieredStoreResultPartition,
            BufferPoolHelper bufferPoolHelper,
            boolean isBroadcastOnly,
            int configuredNetworkBuffersPerChannel) {

        this.subpartitions = checkNotNull(subpartitions);
        this.numSubpartitions = subpartitions.length;
        this.unicastBufferBuilders = new BufferBuilder[subpartitions.length];

        this.allRecordsProcessedSubpartitions = new boolean[subpartitions.length];
        this.numNotAllRecordsProcessedSubpartitions = subpartitions.length;

        this.consumedSubpartitions = new boolean[subpartitions.length];
        this.numberOfUsers = subpartitions.length + 1;
        this.tieredStoreResultPartition = tieredStoreResultPartition;

        this.bufferPoolHelper = bufferPoolHelper;
        this.segmentIndexTracker = new SubpartitionSegmentIndexTracker(numSubpartitions, false);
        this.bufferIndexOfLastRecordInSegment =
                new BufferIndexOfLastRecordInSegmentTracker(numSubpartitions);
        this.finishedBufferIndex = new int[numSubpartitions];
        Arrays.fill(finishedBufferIndex, -1);
        this.isBroadcastOnly = isBroadcastOnly;
        this.configuredNetworkBuffersPerChannel = configuredNetworkBuffersPerChannel;
    }

    //////////////////////// Methods from BufferWritingResultPartition /////////////////////////////

    @Override
    public void setup() throws IOException {
        // TODO check the minimum required buffers if there is only memory gate
        // checkState(
        //        bufferPool.getNumberOfRequiredMemorySegments() >= getNumberOfSubpartitions(),
        //        "Bug in result partition setup logic: Buffer pool has not enough guaranteed
        // buffers for"
        //                + " this result partition.");
        for (int i = 0; i < subpartitions.length; i++) {
            subpartitions[i] =
                    new SubpartitionLocalMemoryDataManager(
                            i,
                            configuredNetworkBuffersPerChannel,
                            tieredStoreResultPartition,
                            this);
        }
    }

    @Override
    public int getNumberOfQueuedBuffers() {
        int totalBuffers = 0;

        for (ResultSubpartition subpartition : subpartitions) {
            totalBuffers += subpartition.unsynchronizedGetNumberOfQueuedBuffers();
        }

        return totalBuffers;
    }

    @Override
    public long getSizeOfQueuedBuffersUnsafe() {
        long totalNumberOfBytes = 0;

        for (ResultSubpartition subpartition : subpartitions) {
            totalNumberOfBytes += Math.max(0, subpartition.getTotalNumberOfBytesUnsafe());
        }

        return totalWrittenBytes - totalNumberOfBytes;
    }

    @Override
    public int getNumberOfQueuedBuffers(int targetSubpartition) {
        checkArgument(targetSubpartition >= 0 && targetSubpartition < numSubpartitions);
        return subpartitions[targetSubpartition].unsynchronizedGetNumberOfQueuedBuffers();
    }

    protected void flushSubpartition(int targetSubpartition, boolean finishProducers) {
        if (finishProducers) {
            finishBroadcastBufferBuilder();
            finishUnicastBufferBuilder(targetSubpartition, false);
        }

        subpartitions[targetSubpartition].flush();
    }

    protected void flushAllSubpartitions(boolean finishProducers) {
        if (finishProducers) {
            finishBroadcastBufferBuilder();
            finishUnicastBufferBuilders();
        }

        for (ResultSubpartition subpartition : subpartitions) {
            subpartition.flush();
        }
    }

    @Override
    public void emit(
            ByteBuffer record,
            int targetSubpartition,
            Buffer.DataType dataType,
            boolean isBroadcast,
            boolean isLastRecordInSegment,
            boolean isEndOfPartition,
            long segmentIndex)
            throws IOException {

        if (isBroadcastOnly) {
            for (int i = 0; i < numSubpartitions; ++i) {
                emitSingleSubpartition(
                        record.duplicate(),
                        i,
                        segmentIndex,
                        dataType,
                        isLastRecordInSegment,
                        isEndOfPartition);
            }
        } else {
            emitSingleSubpartition(
                    record,
                    targetSubpartition,
                    segmentIndex,
                    dataType,
                    isLastRecordInSegment,
                    isEndOfPartition);
        }
    }

    private void emitSingleSubpartition(
            ByteBuffer record,
            int targetSubpartition,
            long segmentIndex,
            Buffer.DataType dataType,
            boolean isLastRecordInSegment,
            boolean isEndOfPartition)
            throws IOException {

        segmentIndexTracker.addSubpartitionSegmentIndex(targetSubpartition, segmentIndex);
        totalWrittenBytes += record.remaining();

        // For record
        if (dataType != Buffer.DataType.EVENT_BUFFER) {
            BufferBuilder buffer = appendUnicastDataForNewRecord(record, targetSubpartition);

            while (record.hasRemaining()) {
                // full buffer, partial record
                finishUnicastBufferBuilder(targetSubpartition, false);
                buffer = appendUnicastDataForRecordContinuation(record, targetSubpartition);
            }

            if (buffer.isFull() || isLastRecordInSegment) {
                // full buffer, full record, or isLastRecordInSegment
                finishUnicastBufferBuilder(targetSubpartition, isLastRecordInSegment);
                // notify available when finished && isLastRecordInSegment
                if (isLastRecordInSegment) {
                    ((SubpartitionLocalMemoryDataManager) subpartitions[targetSubpartition])
                            .notifyAvailable();
                }
            }

            // partial buffer, full record
        } else {
            finishUnicastBufferBuilder(targetSubpartition, false);

            if (isEndOfPartition) {
                synchronized (finishedBufferIndex) {
                    finishedBufferIndex[targetSubpartition]++;
                    if (isLastRecordInSegment) {
                        bufferIndexOfLastRecordInSegment.addBufferIndexOfLastRecordInSegment(
                                targetSubpartition, finishedBufferIndex[targetSubpartition]);
                    }
                }
                subpartitions[targetSubpartition].finish();
                return;
            }
            MemorySegment data = MemorySegmentFactory.wrap(record.array());
            BufferConsumer bufferConsumer =
                    new BufferConsumer(
                            new NetworkBuffer(data, FreeingBufferRecycler.INSTANCE, dataType),
                            data.size());
            synchronized (finishedBufferIndex) {
                finishedBufferIndex[targetSubpartition]++;
                if (isLastRecordInSegment) {
                    bufferIndexOfLastRecordInSegment.addBufferIndexOfLastRecordInSegment(
                            targetSubpartition, finishedBufferIndex[targetSubpartition]);
                }
            }
            subpartitions[targetSubpartition].add(bufferConsumer, 0);
            if (isLastRecordInSegment) {
                ((SubpartitionLocalMemoryDataManager) subpartitions[targetSubpartition])
                        .notifyAvailable();
            }
        }

        // decreaseRedundantBufferNumberInSegment
        if (isLastRecordInSegment) {
            bufferPoolHelper.decreaseRedundantBufferNumberInSegment(
                    targetSubpartition, bufferNumberInSegment);
        }
    }

    @Override
    public boolean isLastRecordInSegment(int subpartitionId, int bufferIndex) {
        return bufferIndexOfLastRecordInSegment.isLastRecordInSegment(subpartitionId, bufferIndex);
    }

    @Override
    public void alignedBarrierTimeout(long checkpointId) throws IOException {
        for (ResultSubpartition subpartition : subpartitions) {
            subpartition.alignedBarrierTimeout(checkpointId);
        }
    }

    @Override
    public void abortCheckpoint(long checkpointId, CheckpointException cause) {
        for (ResultSubpartition subpartition : subpartitions) {
            subpartition.abortCheckpoint(checkpointId, cause);
        }
    }

    @Override
    public void setOutputMetrics(OutputMetrics tieredStoreOutputMetrics) {
        numBuffersOut = tieredStoreOutputMetrics.getNumBuffersOut();
        numBytesOut = tieredStoreOutputMetrics.getNumBytesOut();
    }

    @Override
    public void setTimerGauge(TimerGauge timerGauge) {
        hardBackPressuredTimeMsPerSecond = timerGauge;
    }

    @Override
    public SingleTierReader createSubpartitionTierReader(
            int subpartitionId, BufferAvailabilityListener availabilityListener)
            throws IOException {
        checkElementIndex(subpartitionId, numSubpartitions, "Subpartition not found.");

        ResultSubpartition subpartition = subpartitions[subpartitionId];
        ResultSubpartitionView readView = subpartition.createReadView(availabilityListener);
        return new MemoryReader(readView);
    }

    // Not implemented finish()

    protected void releaseInternal() {
        // Release all subpartitions
        for (ResultSubpartition subpartition : subpartitions) {
            try {
                subpartition.release();
            }
            // Catch this in order to ensure that release is called on all subpartitions
            catch (Throwable t) {
                LOG.error("Error during release of result subpartition: " + t.getMessage(), t);
            }
        }
    }

    // close is implemented by PipelinedResultPartition

    private BufferBuilder appendUnicastDataForNewRecord(
            final ByteBuffer record, final int targetSubpartition) throws IOException {
        if (targetSubpartition < 0 || targetSubpartition > unicastBufferBuilders.length) {
            throw new ArrayIndexOutOfBoundsException(targetSubpartition);
        }
        BufferBuilder buffer = unicastBufferBuilders[targetSubpartition];

        if (buffer == null) {
            buffer = requestNewUnicastBufferBuilder(targetSubpartition);
            addToSubpartition(buffer, targetSubpartition, 0, record.remaining());
        }

        buffer.appendAndCommit(record);

        return buffer;
    }

    private void addToSubpartition(
            BufferBuilder buffer,
            int targetSubpartition,
            int partialRecordLength,
            int minDesirableBufferSize)
            throws IOException {
        int desirableBufferSize =
                subpartitions[targetSubpartition].add(
                        buffer.createBufferConsumerFromBeginning(), partialRecordLength);

        resizeBuffer(buffer, desirableBufferSize, minDesirableBufferSize);
    }

    private void resizeBuffer(
            BufferBuilder buffer, int desirableBufferSize, int minDesirableBufferSize) {
        if (desirableBufferSize > 0) {
            // !! If some of partial data has written already to this buffer, the result size can
            // not be less than written value.
            buffer.trim(Math.max(minDesirableBufferSize, desirableBufferSize));
        }
    }

    private BufferBuilder appendUnicastDataForRecordContinuation(
            final ByteBuffer remainingRecordBytes, final int targetSubpartition)
            throws IOException {
        final BufferBuilder buffer = requestNewUnicastBufferBuilder(targetSubpartition);
        // !! Be aware, in case of partialRecordBytes != 0, partial length and data has to
        // `appendAndCommit` first
        // before consumer is created. Otherwise it would be confused with the case the buffer
        // starting
        // with a complete record.
        // !! The next two lines can not change order.
        final int partialRecordBytes = buffer.appendAndCommit(remainingRecordBytes);
        addToSubpartition(buffer, targetSubpartition, partialRecordBytes, partialRecordBytes);

        return buffer;
    }

    private BufferBuilder requestNewUnicastBufferBuilder(int targetSubpartition) {
        tieredStoreResultPartition.checkInProduceState();
        ensureUnicastMode();
        final BufferBuilder bufferBuilder = requestNewBufferBuilderFromPool(targetSubpartition);
        unicastBufferBuilders[targetSubpartition] = bufferBuilder;

        return bufferBuilder;
    }

    private BufferBuilder requestNewBufferBuilderFromPool(int targetSubpartition) {
        MemorySegment memorySegment =
                bufferPoolHelper.requestMemorySegmentBlocking(
                        targetSubpartition, TieredStoreMode.TieredType.IN_MEM, true);
        return new BufferBuilder(
                memorySegment,
                ms -> {
                    bufferPoolHelper.recycleBuffer(
                            targetSubpartition, ms, TieredStoreMode.TieredType.IN_MEM, true);
                });

        // 反压显示
        // BufferBuilder bufferBuilder = bufferPool.requestBufferBuilder(targetSubpartition);
        // if (bufferBuilder != null) {
        //   return bufferBuilder;
        // }
        //
        // hardBackPressuredTimeMsPerSecond.markStart();
        // try {
        //   bufferBuilder = bufferPool.requestBufferBuilderBlocking(targetSubpartition);
        //   hardBackPressuredTimeMsPerSecond.markEnd();
        //   return bufferBuilder;
        // } catch (InterruptedException e) {
        //   throw new IOException("Interrupted while waiting for buffer");
        // }

    }

    private void finishUnicastBufferBuilder(int targetSubpartition, boolean isLastBufferInSegment) {
        final BufferBuilder bufferBuilder = unicastBufferBuilders[targetSubpartition];
        if (bufferBuilder != null) {
            synchronized (finishedBufferIndex) {
                finishedBufferIndex[targetSubpartition]++;
            }
            if (isLastBufferInSegment) {
                synchronized (finishedBufferIndex) {
                    bufferIndexOfLastRecordInSegment.addBufferIndexOfLastRecordInSegment(
                            targetSubpartition, finishedBufferIndex[targetSubpartition]);
                }
            }
            int bytes = bufferBuilder.finish();
            numBytesProduced.inc(bytes);
            numBytesOut.inc(bytes);
            numBuffersOut.inc();
            unicastBufferBuilders[targetSubpartition] = null;
            bufferBuilder.close();
        }
    }

    private void finishUnicastBufferBuilders() {
        for (int channelIndex = 0; channelIndex < numSubpartitions; channelIndex++) {
            finishUnicastBufferBuilder(channelIndex, false);
        }
    }

    private void finishBroadcastBufferBuilder() {
        if (broadcastBufferBuilder != null) {
            int bytes = broadcastBufferBuilder.finish();
            numBytesProduced.inc(bytes);
            numBytesOut.inc(bytes * numSubpartitions);
            numBuffersOut.inc(numSubpartitions);
            broadcastBufferBuilder.close();
            broadcastBufferBuilder = null;
        }
    }

    private void ensureUnicastMode() {
        finishBroadcastBufferBuilder();
    }

    @VisibleForTesting
    public TimerGauge getHardBackPressuredTimeMsPerSecond() {
        return hardBackPressuredTimeMsPerSecond;
    }

    @VisibleForTesting
    public ResultSubpartition[] getAllPartitions() {
        return subpartitions;
    }

    ////////////////////////// Methods from PipelinedResultPartition ///////////////////////////////

    @Override
    public void setChannelStateWriter(ChannelStateWriter channelStateWriter) {
        for (final ResultSubpartition subpartition : subpartitions) {
            if (subpartition instanceof ChannelStateHolder) {
                ((SubpartitionLocalMemoryDataManager) subpartition)
                        .setChannelStateWriter(channelStateWriter);
            }
        }
    }

    /**
     * The pipelined partition releases automatically once all subpartition readers are released.
     * That is because pipelined partitions cannot be consumed multiple times, or reconnect.
     */
    @Override
    public void onConsumedSubpartition(int subpartitionIndex) {
        decrementNumberOfUsers(subpartitionIndex);
    }

    private void decrementNumberOfUsers(int subpartitionIndex) {
        if (tieredStoreResultPartition.isReleased()) {
            return;
        }

        final int remainingUnconsumed;

        // we synchronize only the bookkeeping section, to avoid holding the lock during any
        // calls into other components
        synchronized (lock) {
            if (subpartitionIndex != PIPELINED_RESULT_PARTITION_ITSELF) {
                if (consumedSubpartitions[subpartitionIndex]) {
                    // repeated call - ignore
                    return;
                }

                consumedSubpartitions[subpartitionIndex] = true;
            }
            remainingUnconsumed = (--numberOfUsers);
        }

        LOG.debug(
                "{}: Received consumed notification for subpartition {}.", this, subpartitionIndex);

        if (remainingUnconsumed == 0) {
            tieredStoreResultPartition
                    .getPartitionManager()
                    .onConsumedPartition(tieredStoreResultPartition);
        } else if (remainingUnconsumed < 0) {
            throw new IllegalStateException(
                    "Received consume notification even though all subpartitions are already consumed.");
        }
    }

    @Override
    public CheckpointedResultSubpartition getCheckpointedSubpartition(int subpartitionIndex) {
        return (CheckpointedResultSubpartition) subpartitions[subpartitionIndex];
    }

    @Override
    public void flushAll() {
        flushAllSubpartitions(false);
    }

    @Override
    public void flush(int targetSubpartition) {
        flushSubpartition(targetSubpartition, false);
    }

    // not implemented notifyEndOfData

    @Override
    public CompletableFuture<Void> getAllDataProcessedFuture() {
        return allRecordsProcessedFuture;
    }

    @Override
    public void onSubpartitionAllDataProcessed(int subpartition) {
        synchronized (lock) {
            if (allRecordsProcessedSubpartitions[subpartition]) {
                return;
            }

            allRecordsProcessedSubpartitions[subpartition] = true;
            numNotAllRecordsProcessedSubpartitions--;

            if (numNotAllRecordsProcessedSubpartitions == 0) {
                allRecordsProcessedFuture.complete(null);
            }
        }
    }

    // not implemented checkResultPartitionType

    @Override
    public void finishReadRecoveredState(boolean notifyAndBlockOnCompletion) throws IOException {
        for (ResultSubpartition subpartition : subpartitions) {
            ((CheckpointedResultSubpartition) subpartition)
                    .finishReadRecoveredState(notifyAndBlockOnCompletion);
        }
    }

    @Override
    public void close() {
        if (isClosed) {
            return;
        }
        decrementNumberOfUsers(PIPELINED_RESULT_PARTITION_ITSELF);

        // We can not close these buffers in the release method because of the potential race
        // condition. This close method will be only called from the Task thread itself.
        if (broadcastBufferBuilder != null) {
            broadcastBufferBuilder.close();
            broadcastBufferBuilder = null;
        }
        for (int i = 0; i < unicastBufferBuilders.length; ++i) {
            if (unicastBufferBuilders[i] != null) {
                unicastBufferBuilders[i].close();
                unicastBufferBuilders[i] = null;
            }
        }
        isClosed = true;
    }

    /////////////////////////// Methods from LocalMemoryDataManager ////////////////////////////////

    @Override
    public SingleTierWriter createPartitionTierWriter() throws IOException {
        return this;
    }

    @Override
    public boolean canStoreNextSegment(int subpartitionId) {
        return bufferPoolHelper.canStoreNextSegmentForMemoryTier(bufferNumberInSegment);
    }

    @Override
    public int getNewSegmentSize() {
        return numBytesInASegment;
    }

    @Override
    public void setNumBytesInASegment(int numBytesInASegment) {
        this.numBytesInASegment = numBytesInASegment;
    }

    @Override
    public boolean hasCurrentSegment(int subpartitionId, long segmentIndex) {
        return segmentIndexTracker.hasCurrentSegment(subpartitionId, segmentIndex);
    }

    @Override
    public void release() {
        if (!isReleased) {
            releaseInternal();
        }
        isReleased = true;
    }
}
