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

package org.apache.flink.runtime.io.network.partition.store.common;

import org.apache.flink.runtime.io.network.partition.ProducerFailedException;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * The Reader of Single Tier.
 */
public interface SingleTierReader {

    @Nullable
    ResultSubpartition.BufferAndBacklog getNextBuffer() throws IOException;

    ResultSubpartitionView.AvailabilityWithBacklog getAvailabilityAndBacklog(
            int numCreditsAvailable);

    void releaseAllResources() throws IOException;

    boolean isReleased();

    /**
     * {@link ResultSubpartitionView} can decide whether the failure cause should be reported to
     * consumer as failure (primary failure) or {@link ProducerFailedException} (secondary failure).
     * Secondary failure can be reported only if producer (upstream task) is guaranteed to failover.
     *
     * <p><strong>BEWARE:</strong> Incorrectly reporting failure cause as primary failure, can hide
     * the root cause of the failure from the user.
     */
    Throwable getFailureCause();

    int unsynchronizedGetNumberOfQueuedBuffers();

    int getNumberOfQueuedBuffers();
}
