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

package org.apache.flink.runtime.io.network.partition.store;

import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.store.common.SingleTierDataGate;
import org.apache.flink.runtime.io.network.partition.store.common.TieredStoreConsumer;
import org.apache.flink.runtime.io.network.partition.store.reader.TieredStoreConsumerImpl;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkArgument;

class TieredStoreSubpartitionViewDelegate implements ResultSubpartitionView {

    private final TieredStoreConsumer storeConsumer;

    private String taskName;

    TieredStoreSubpartitionViewDelegate(
            int subpartitionId,
            BufferAvailabilityListener availabilityListener,
            SingleTierDataGate[] tierDataGates,
            String taskName)
            throws IOException {
        checkArgument(tierDataGates.length > 0, "Empty tier transmitters.");

        this.storeConsumer =
                new TieredStoreConsumerImpl(
                        subpartitionId, availabilityListener, tierDataGates, taskName);
        this.taskName = taskName;
    }

    @Nullable
    @Override
    public ResultSubpartition.BufferAndBacklog getNextBuffer() throws IOException {
        return storeConsumer.getNextBuffer();
    }

    @Override
    public void notifyDataAvailable() {
        throw new UnsupportedOperationException(
                "Method notifyDataAvailable should never be called.");
    }

    @Override
    public void releaseAllResources() throws IOException {
        storeConsumer.releaseAllResources();
    }

    @Override
    public boolean isReleased() {
        return storeConsumer.isReleased();
    }

    @Override
    public void resumeConsumption() {
        throw new UnsupportedOperationException("Method resumeConsumption should never be called.");
    }

    @Override
    public void acknowledgeAllDataProcessed() {
        // in case of bounded partitions there is no upstream to acknowledge, we simply ignore
        // the ack, as there are no checkpoints
    }

    @Override
    public Throwable getFailureCause() {
        return storeConsumer.getFailureCause();
    }

    @Override
    public AvailabilityWithBacklog getAvailabilityAndBacklog(int numCreditsAvailable) {
        return storeConsumer.getAvailabilityAndBacklog(numCreditsAvailable);
    }

    @Override
    public int unsynchronizedGetNumberOfQueuedBuffers() {
        return storeConsumer.unsynchronizedGetNumberOfQueuedBuffers();
    }

    @Override
    public int getNumberOfQueuedBuffers() {
        return storeConsumer.getNumberOfQueuedBuffers();
    }

    @Override
    public void notifyNewBufferSize(int newBufferSize) {
        throw new UnsupportedOperationException(
                "Method notifyNewBufferSize should never be called.");
    }

    @Override
    public String getTaskName() {
        return taskName;
    }

    @Override
    public boolean containSegment(long segmentId) {
        return storeConsumer.containSegment(segmentId);
    }

    @Override
    public void notifyRequiredSegmentId(long segmentId) {
        storeConsumer.updateConsumedSegmentIndex(segmentId);
        storeConsumer.forceNotifyAvailable();
    }
}
