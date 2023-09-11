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

import javax.annotation.concurrent.GuardedBy;

import java.util.Deque;
import java.util.LinkedList;
import java.util.Optional;
import java.util.Queue;

import static org.apache.flink.util.Preconditions.checkState;

/** {@link NettyPayloadManager} is used to contain all netty payloads from a storage tier. */
public class NettyPayloadManager {

    private final Object lock = new Object();

    private final Queue<NettyPayload> queue = new LinkedList<>();

    @GuardedBy("lock")
    private final Deque<Integer> segmentBacklogQueue = new LinkedList<>();

    public void add(NettyPayload nettyPayload) {
        synchronized (lock) {
            queue.add(nettyPayload);
            if (nettyPayload.getSegmentId() >= 0) {
                segmentBacklogQueue.add(0);
            }
            Optional<Buffer> buffer = nettyPayload.getBuffer();
            if (buffer.isPresent() && buffer.get().isBuffer()) {
                checkState(!segmentBacklogQueue.isEmpty());
                segmentBacklogQueue.addFirst(segmentBacklogQueue.pollFirst() + 1);
            }
        }
    }

    public NettyPayload peek() {
        synchronized (lock) {
            return queue.peek();
        }
    }

    public NettyPayload poll() {
        synchronized (lock) {
            NettyPayload nettyPayload = queue.poll();
            if (nettyPayload == null || !nettyPayload.getBuffer().isPresent()) {
                return nettyPayload;
            }

            Buffer buffer = nettyPayload.getBuffer().get();
            if (buffer.getDataType() == Buffer.DataType.END_OF_SEGMENT) {
                checkState(!segmentBacklogQueue.isEmpty() && segmentBacklogQueue.peek() == 0);
                segmentBacklogQueue.poll();
            }
            if (buffer.isBuffer()) {
                checkState(!segmentBacklogQueue.isEmpty());
                int currentBacklog = segmentBacklogQueue.pollLast() - 1;
                checkState(currentBacklog >= 0);
                segmentBacklogQueue.addLast(currentBacklog);
            }
            return nettyPayload;
        }
    }

    public int getBacklog() {
        synchronized (lock) {
            return segmentBacklogQueue.isEmpty() ? 0 : segmentBacklogQueue.peek();
        }
    }

    public int getSize() {
        synchronized (lock) {
            return queue.size();
        }
    }
}
