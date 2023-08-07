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

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/** The queue containing data buffers. */
public class NettyPayloadQueue {

    private final Queue<NettyPayload> queue = new LinkedBlockingQueue<>();

    private final AtomicInteger backlog = new AtomicInteger(0);

    public void add(NettyPayload nettyPayload) {
        queue.add(nettyPayload);
        if (nettyPayload.getBuffer().isPresent()) {
            backlog.getAndIncrement();
        }
    }

    public NettyPayload peek() {
        return queue.peek();
    }

    public NettyPayload poll() {
        NettyPayload nettyPayload = queue.poll();
        if (nettyPayload != null && nettyPayload.getBuffer().isPresent()) {
            backlog.getAndDecrement();
        }
        return nettyPayload;
    }

    public int getBacklog() {
        return backlog.get();
    }
}
