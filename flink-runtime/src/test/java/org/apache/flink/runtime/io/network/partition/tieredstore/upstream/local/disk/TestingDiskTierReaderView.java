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

package org.apache.flink.runtime.io.network.partition.tieredstore.upstream.local.disk;

import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierReader;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierReaderView;

import javax.annotation.Nullable;

import java.io.IOException;

/** Mock {@link TierReaderView} for test. */
public class TestingDiskTierReaderView implements TierReaderView {

    // -1 indicates downstream just start consuming offset.
    private int consumingOffset = -1;

    private Runnable notifyDataAvailableRunnable = () -> {};

    @Override
    public void notifyDataAvailable() {
        notifyDataAvailableRunnable.run();
    }

    @Override
    public int getConsumingOffset(boolean withLock) {
        return consumingOffset;
    }

    public void advanceConsumptionProgress() {
        consumingOffset++;
    }

    public void setNotifyDataAvailableRunnable(Runnable notifyDataAvailableRunnable) {
        this.notifyDataAvailableRunnable = notifyDataAvailableRunnable;
    }

    @Override
    public void updateNeedNotifyStatus() {}

    @Nullable
    @Override
    public ResultSubpartition.BufferAndBacklog getNextBuffer() throws IOException {
        return null;
    }

    @Override
    public ResultSubpartitionView.AvailabilityWithBacklog getAvailabilityAndBacklog(
            int numCreditsAvailable) {
        return null;
    }

    @Override
    public Throwable getFailureCause() {
        return null;
    }

    @Override
    public int unsynchronizedGetNumberOfQueuedBuffers() {
        return 0;
    }

    @Override
    public int getNumberOfQueuedBuffers() {
        return 0;
    }

    @Override
    public boolean isReleased() {
        return false;
    }

    @Override
    public void releaseAllResources() throws IOException {}

    @Override
    public void setTierReader(TierReader tierReader) {}
}
