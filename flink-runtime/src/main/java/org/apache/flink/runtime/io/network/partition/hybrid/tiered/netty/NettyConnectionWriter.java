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

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.impl.NettyConnectionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.BufferContext;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierProducerAgent;

/** {@link NettyConnectionWriter} is used by {@link TierProducerAgent} to write buffer to netty. */
public interface NettyConnectionWriter {
    /**
     * Write a buffer.
     *
     * @param bufferContext buffer context represents the buffer.
     */
    void writeBuffer(BufferContext bufferContext);


    NettyConnectionId getNettyConnectionId();


    /**
     * Get the number of existed buffers in the writer.
     *
     * @return the buffer number.
     */
    int size();

    /** Close the writer and recycle all buffers. */
    void close();
}
