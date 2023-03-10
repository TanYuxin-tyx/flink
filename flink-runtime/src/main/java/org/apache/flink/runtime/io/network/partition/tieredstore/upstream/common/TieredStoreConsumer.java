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

/** The consumer interface of Tiered Store, data will be read from different store tiers. */
public interface TieredStoreConsumer {

    @Nullable
    ResultSubpartition.BufferAndBacklog getNextBuffer() throws IOException;

    ResultSubpartitionView.AvailabilityWithBacklog getAvailabilityAndBacklog(
            int numCreditsAvailable);

    int unsynchronizedGetNumberOfQueuedBuffers();

    int getNumberOfQueuedBuffers();

    void forceNotifyAvailable();

    void updateConsumedSegmentId(int segmentId);

    boolean isReleased();

    void releaseAllResources() throws IOException;

    Throwable getFailureCause();
}
