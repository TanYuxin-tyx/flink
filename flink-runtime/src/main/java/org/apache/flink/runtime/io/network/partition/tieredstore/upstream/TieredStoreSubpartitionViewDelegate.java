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

package org.apache.flink.runtime.io.network.partition.tieredstore.upstream;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.StorageTier;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.service.TieredStoreNettyService;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.service.TieredStoreNettyServiceImpl;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkArgument;

class TieredStoreSubpartitionViewDelegate implements ResultSubpartitionView {

    private final TieredStoreNettyService nettyService;

    TieredStoreSubpartitionViewDelegate(
            int subpartitionId,
            BufferAvailabilityListener availabilityListener,
            StorageTier[] tiers)
            throws IOException {
        checkArgument(tiers.length > 0, "Empty tier transmitters.");
        this.nettyService =
                new TieredStoreNettyServiceImpl(subpartitionId, availabilityListener, tiers);
        nettyService.start();
    }

    @Nullable
    @Override
    public ResultSubpartition.BufferAndBacklog getNextBuffer() throws IOException {
        return nettyService.getNextBuffer();
    }

    @Override
    public void notifyDataAvailable() {
        throw new UnsupportedOperationException(
                "Method notifyDataAvailable should never be called.");
    }

    @Override
    public void releaseAllResources() throws IOException {
        nettyService.close();
    }

    @Override
    public boolean isReleased() {
        return nettyService.isClosed();
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
        return nettyService.getFailureCause();
    }

    @Override
    public AvailabilityWithBacklog getAvailabilityAndBacklog(int numCreditsAvailable) {
        return nettyService.getAvailabilityAndBacklog(numCreditsAvailable);
    }

    @Override
    public int unsynchronizedGetNumberOfQueuedBuffers() {
        return nettyService.unsynchronizedGetNumberOfQueuedBuffers();
    }

    @Override
    public int getNumberOfQueuedBuffers() {
        return nettyService.getNumberOfQueuedBuffers();
    }

    @Override
    public void notifyNewBufferSize(int newBufferSize) {
        throw new UnsupportedOperationException(
                "Method notifyNewBufferSize should never be called.");
    }

    @Override
    public void notifyRequiredSegmentId(int segmentId) {
        nettyService.updateRequiredSegmentId(segmentId);
        nettyService.forceNotifyAvailable();
    }

    @VisibleForTesting
    public TieredStoreNettyService getNettyService() {
        return nettyService;
    }
}
