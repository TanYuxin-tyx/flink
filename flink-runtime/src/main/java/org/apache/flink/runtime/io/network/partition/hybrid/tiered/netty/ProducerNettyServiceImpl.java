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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty;

import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.BufferContext;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

/** The default implementation of {@link ProducerNettyService}. */
public class ProducerNettyServiceImpl implements ProducerNettyService {

    private final Map<Integer, Queue<BufferContext>> registeredBufferQueues = new HashMap<>();

    private final Map<Integer, Runnable> registeredReleaseNotifiers = new HashMap<>();

    @Override
    public void register(
            int subpartitionId, Queue<BufferContext> bufferQueue, Runnable serviceReleaseNotifier) {
        registeredBufferQueues.put(subpartitionId, bufferQueue);
        registeredReleaseNotifiers.put(subpartitionId, serviceReleaseNotifier);
    }

    @Override
    public CreditBasedBufferQueueView createCreditBasedBufferQueueView(
            int subpartitionId, BufferAvailabilityListener availabilityListener) {
        Queue<BufferContext> bufferQueue = registeredBufferQueues.get(subpartitionId);
        Runnable releaseNotifier = registeredReleaseNotifiers.get(subpartitionId);
        registeredBufferQueues.remove(subpartitionId);
        registeredReleaseNotifiers.remove(subpartitionId);
        return new CreditBasedBufferQueueViewImpl(bufferQueue, availabilityListener, releaseNotifier);
    }
}
