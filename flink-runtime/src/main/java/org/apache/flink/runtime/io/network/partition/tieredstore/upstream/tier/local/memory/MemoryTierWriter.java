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

package org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.memory;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.TieredStoreMode;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.EndOfSegmentEventBuilder;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.SubpartitionSegmentIndexTracker;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierReader;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierReaderView;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierReaderViewId;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierWriter;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreMemoryManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.OutputMetrics;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.SEGMENT_EVENT;

/** This class is responsible for managing cached buffers data before flush to local files. */
public class MemoryTierWriter implements TierWriter, MemoryDataWriterOperation {

    private final int numSubpartitions;

    private final SubpartitionMemoryDataManager[] subpartitionMemoryDataManagers;

    private final TieredStoreMemoryManager tieredStoreMemoryManager;

    /**
     * Each element of the list is all views of the subpartition corresponding to its index, which
     * are stored in the form of a map that maps consumer id to its subpartition view.
     */
    private final List<Map<TierReaderViewId, TierReaderView>> subpartitionViewOperationsMap;

    // Record the byte number currently written to each sub partition.
    private final int[] numSubpartitionEmitBytes;

    private final SubpartitionSegmentIndexTracker subpartitionSegmentIndexTracker;

    private final boolean isBroadcastOnly;

    private final int numTotalConsumers;

    private int numBytesInASegment;

    public MemoryTierWriter(
            int numSubpartitions,
            int bufferSize,
            TieredStoreMemoryManager tieredStoreMemoryManager,
            BufferCompressor bufferCompressor,
            SubpartitionSegmentIndexTracker subpartitionSegmentIndexTracker,
            boolean isBroadcastOnly,
            int numTotalConsumers,
            int numBytesInASegment) {
        this.numSubpartitions = numSubpartitions;
        this.numTotalConsumers = numTotalConsumers;
        this.tieredStoreMemoryManager = tieredStoreMemoryManager;
        this.subpartitionMemoryDataManagers = new SubpartitionMemoryDataManager[numSubpartitions];
        this.subpartitionSegmentIndexTracker = subpartitionSegmentIndexTracker;
        this.isBroadcastOnly = isBroadcastOnly;
        this.numSubpartitionEmitBytes = new int[numSubpartitions];
        Arrays.fill(numSubpartitionEmitBytes, 0);
        this.numBytesInASegment = numBytesInASegment;

        this.subpartitionViewOperationsMap = new ArrayList<>(numSubpartitions);
        for (int subpartitionId = 0; subpartitionId < numSubpartitions; ++subpartitionId) {
            subpartitionMemoryDataManagers[subpartitionId] =
                    new SubpartitionMemoryDataManager(
                            subpartitionId,
                            bufferSize,
                            numTotalConsumers,
                            bufferCompressor,
                            this,
                            tieredStoreMemoryManager);
            subpartitionViewOperationsMap.add(new ConcurrentHashMap<>());
        }
    }

    @Override
    public void setup() throws IOException {}

    @Override
    public boolean emitBuffer(
            int targetSubpartition,
            Buffer finishedBuffer,
            boolean isBroadcast,
            boolean isEndOfPartition,
            int segmentId)
            throws IOException {
        boolean isLastBufferInSegment = false;
        numSubpartitionEmitBytes[targetSubpartition] += finishedBuffer.readableBytes();
        if (numSubpartitionEmitBytes[targetSubpartition] >= numBytesInASegment) {
            isLastBufferInSegment = true;
            numSubpartitionEmitBytes[targetSubpartition] = 0;
        }
        subpartitionSegmentIndexTracker.addSubpartitionSegmentIndex(targetSubpartition, segmentId);
        if (isLastBufferInSegment && !isEndOfPartition) {
            append(finishedBuffer, targetSubpartition, isBroadcast && isBroadcastOnly, false);
            // Send the EndOfSegmentEvent
            ByteBuffer endOfSegment =
                    EndOfSegmentEventBuilder.buildEndOfSegmentEvent(segmentId + 1);
            append(
                    endOfSegment,
                    targetSubpartition,
                    SEGMENT_EVENT,
                    isBroadcast && isBroadcastOnly,
                    true);
        } else {
            append(
                    finishedBuffer,
                    targetSubpartition,
                    isBroadcast && isBroadcastOnly,
                    isLastBufferInSegment);
        }
        return isLastBufferInSegment;
    }

    private void append(
            ByteBuffer record,
            int targetChannel,
            Buffer.DataType dataType,
            boolean isBroadcast,
            boolean isLastBufferInSegment)
            throws IOException {
        try {
            getSubpartitionMemoryDataManager(targetChannel)
                    .append(record, dataType, isBroadcast, isLastBufferInSegment);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    private void append(
            Buffer finishedBuffer,
            int targetChannel,
            boolean isBroadcast,
            boolean isLastBufferInSegment)
            throws IOException {
        getSubpartitionMemoryDataManager(targetChannel)
                .addFinishedBuffer(isBroadcast, finishedBuffer);
    }

    public TierReader registerNewConsumer(
            int subpartitionId, TierReaderViewId tierReaderViewId, TierReaderView viewOperations) {
        TierReaderView oldView =
                subpartitionViewOperationsMap
                        .get(subpartitionId)
                        .put(tierReaderViewId, viewOperations);
        Preconditions.checkState(
                oldView == null, "Each subpartition view should have unique consumerId.");
        return getSubpartitionMemoryDataManager(subpartitionId)
                .registerNewConsumer(tierReaderViewId);
    }

    /** Close this {@link MemoryTierWriter}, it means no data will be appended to memory. */
    @Override
    public void close() {}

    /**
     * Release this {@link MemoryTierWriter}, it means all memory taken by this class will recycle.
     */
    @Override
    public void release() {
        for (int i = 0; i < numSubpartitions; i++) {
            getSubpartitionMemoryDataManager(i).release();
        }
    }

    public void setOutputMetrics(OutputMetrics metrics) {
        // HsOutputMetrics is not thread-safe. It can be shared by all the subpartitions because it
        // is expected always updated from the producer task's mailbox thread.
        for (int i = 0; i < numSubpartitions; i++) {
            getSubpartitionMemoryDataManager(i).setOutputMetrics(metrics);
        }
    }

    public boolean isConsumerRegistered(int subpartitionId) {
        int numConsumers = subpartitionViewOperationsMap.get(subpartitionId).size();
        if (isBroadcastOnly) {
            return numConsumers == numTotalConsumers;
        }
        return numConsumers > 0;
    }

    @Override
    public void setNumBytesInASegment(int numBytesInASegment) {
        this.numBytesInASegment = numBytesInASegment;
    }

    // ------------------------------------
    //      Callback for subpartition
    // ------------------------------------

    @Override
    public MemorySegment requestBufferFromPool(int subpartitionId) throws InterruptedException {
        return tieredStoreMemoryManager.requestMemorySegmentBlocking(
                TieredStoreMode.TieredType.IN_MEM);
    }

    @Override
    public void onDataAvailable(
            int subpartitionId, Collection<TierReaderViewId> tierReaderViewIds) {
        Map<TierReaderViewId, TierReaderView> consumerViewMap =
                subpartitionViewOperationsMap.get(subpartitionId);
        tierReaderViewIds.forEach(
                consumerId -> {
                    TierReaderView tierReaderView = consumerViewMap.get(consumerId);
                    if (tierReaderView != null) {
                        tierReaderView.notifyDataAvailable();
                    }
                });
    }

    @Override
    public void onConsumerReleased(int subpartitionId, TierReaderViewId tierReaderViewId) {
        subpartitionViewOperationsMap.get(subpartitionId).remove(tierReaderViewId);
        getSubpartitionMemoryDataManager(subpartitionId).releaseConsumer(tierReaderViewId);
    }

    // ------------------------------------
    //           Internal Method
    // ------------------------------------

    private SubpartitionMemoryDataManager getSubpartitionMemoryDataManager(int targetChannel) {
        return subpartitionMemoryDataManagers[targetChannel];
    }
}
