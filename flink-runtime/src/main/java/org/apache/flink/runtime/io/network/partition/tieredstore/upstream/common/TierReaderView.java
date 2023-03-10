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

package org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common;

import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;

import javax.annotation.Nullable;

import java.io.IOException;

/** The {@link TierReaderView} is the view of {@link TierReader}. */
public interface TierReaderView {

    @Nullable
    ResultSubpartition.BufferAndBacklog getNextBuffer() throws IOException;

    ResultSubpartitionView.AvailabilityWithBacklog getAvailabilityAndBacklog(
            int numCreditsAvailable);

    void notifyDataAvailable();

    default int getConsumingOffset(boolean withLock) {
        return -1;
    }

    Throwable getFailureCause();

    void setTierReader(TierReader tierReader);

    int unsynchronizedGetNumberOfQueuedBuffers();

    int getNumberOfQueuedBuffers();

    boolean isReleased();

    void releaseAllResources() throws IOException;
}
