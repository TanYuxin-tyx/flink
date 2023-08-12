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

import java.util.Deque;
import java.util.LinkedList;
import java.util.Optional;
import java.util.Queue;

import static org.apache.flink.shaded.curator5.com.google.common.base.Preconditions.checkState;

/** The queue containing data buffers. */
public class NettyPayloadQueue {

    private final Object lock = new Object();

    private final Queue<NettyPayload> queue = new LinkedList<>();

    private final Deque<Integer> backlogs = new LinkedList<>();

    private int latestSegmentId = -1;

    private boolean shouldAddNewBacklog = false;

    public void add(NettyPayload nettyPayload) {
        synchronized (lock) {
            queue.add(nettyPayload);
            int segmentId = nettyPayload.getSegmentId();
            if (segmentId == 0 || segmentId != -1 && segmentId != (latestSegmentId + 1)) {
                shouldAddNewBacklog = true;
                latestSegmentId = segmentId;
            }
            Optional<Buffer> buffer = nettyPayload.getBuffer();
            if (buffer.isPresent() && buffer.get().isBuffer()) {
                increaseBacklog();
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
            if (nettyPayload != null
                    && nettyPayload.getBuffer().isPresent()
                    && nettyPayload.getBuffer().get().isBuffer()) {
                decreaseBacklog();
            }
            return nettyPayload;
        }
    }

    public int getBacklog() {
        synchronized (lock) {
            if (backlogs.isEmpty()) {
                return 0;
            }
            Integer backlog = backlogs.peekFirst();
            checkState(backlog >= 1);
            return backlog;
        }
    }

    public int getSize() {
        synchronized (lock) {
            return queue.size();
        }
    }

    private void increaseBacklog() {
        if (backlogs.isEmpty() || shouldAddNewBacklog) {
            backlogs.addLast(1);
            shouldAddNewBacklog = false;
        } else {
            backlogs.addLast(backlogs.pollLast() + 1);
        }
    }

    private void decreaseBacklog() {
        Integer currentBacklog = backlogs.pollFirst();
        checkState(currentBacklog != null && currentBacklog >= 1);
        if (currentBacklog > 1) {
            backlogs.addFirst(currentBacklog - 1);
        }
    }
}
