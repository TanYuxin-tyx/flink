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

import java.util.Queue;

/** {@link ProducerNettyService} is used to transfer buffer to netty server in producer side. */
public interface ProducerNettyService {

    /**
     * Register a buffer queue related to a specific subpartition and the buffer will be consumed
     * from the queue and sent to netty server.
     *
     * @param bufferQueue is the required buffer queue.
     * @param serviceReleaseNotifier is used to notify that the service is released.
     */
    void register(
            int subpartitionId, Queue<BufferContext> bufferQueue, Runnable serviceReleaseNotifier);

    /**
     * Create a view of buffer queue in the specific subpartition, which will be used to transfer
     * buffer in netty server with the credit-based protocol.
     *
     * @param subpartitionId subpartition id indicates the id of subpartition.
     * @param availabilityListener availabilityListener is used to listen the available status of
     *     buffer queue.
     * @return the view of buffer queue with the credit-based protocol.
     */
    CreditBasedBufferQueueView createCreditBasedBufferQueueView(
            int subpartitionId, BufferAvailabilityListener availabilityListener);
}
