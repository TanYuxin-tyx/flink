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

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.BufferContext;

import java.util.Optional;
import java.util.Queue;

/**
 * The {@link NettyService} is used to provide the netty-based network services in the shuffle
 * process of tiered-store.
 */
public interface NettyService {

    // ------------------------------------
    //        For Producer Side
    // ------------------------------------

    NettyServiceView register(
            Queue<BufferContext> bufferQueue,
            BufferAvailabilityListener availabilityListener,
            Runnable serviceReleaseNotifier);

    // ------------------------------------
    //        For Consumer Side
    // ------------------------------------

    Optional<Buffer> readBuffer(int subpartitionId);

    void notifyResultSubpartitionAvailable(int subpartitionId, boolean priority);

    void notifyRequiredSegmentId(int subpartitionId, int segmentId);
}
