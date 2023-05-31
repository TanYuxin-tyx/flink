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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyServiceWriterId;

import java.io.IOException;

/**
 * The gate for a single tiered data. The gate is used to create {@link TierProducerAgentWriter}.
 * The writing and reading data processes happen in the writer and reader.
 */
public interface TierProducerAgent {

    void release();

    // forceUseCurrentTier is only for the tests
    boolean tryStartNewSegment(
            TieredStorageSubpartitionId subpartitionId, int segmentId, boolean forceUseCurrentTier);

    boolean write(int consumerId, Buffer finishedBuffer) throws IOException;

    void registerNettyService(
            int subpartitionId, NettyServiceWriterId nettyServiceWriterId) throws IOException;

    void close();
}
