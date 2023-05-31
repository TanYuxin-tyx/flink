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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.impl;

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyServiceWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.BufferContext;

import java.util.Queue;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The default implementation of {@link NettyServiceWriter}. */
public class NettyServiceWriterImpl implements NettyServiceWriter {

    private final Queue<BufferContext> bufferQueue;

    public NettyServiceWriterImpl(Queue<BufferContext> bufferQueue) {
        this.bufferQueue = bufferQueue;
    }

    @Override
    public int size() {
        return bufferQueue.size();
    }

    @Override
    public void writeBuffer(BufferContext bufferContext) {
        bufferQueue.add(bufferContext);
    }

    @Override
    public void clear() {
        BufferContext bufferContext;
        while ((bufferContext = bufferQueue.poll()) != null) {
            if (bufferContext.getBuffer() != null) {
                checkNotNull(bufferContext.getBuffer()).recycleBuffer();
            }
        }
    }
}
