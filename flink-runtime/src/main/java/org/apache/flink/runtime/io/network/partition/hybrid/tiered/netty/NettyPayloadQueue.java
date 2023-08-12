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

import java.util.LinkedList;
import java.util.Queue;

import static org.apache.flink.util.Preconditions.checkState;

/** The queue containing data buffers. */
public class NettyPayloadQueue {

    private final Object lock = new Object();

    private final Queue<NettyPayload> queue = new LinkedList<>();

    private int currentSegmentBacklog;

    private final Queue<Integer> segmentBacklogQueue = new LinkedList<>();

    private int headSegmentBacklog;

    public void add(NettyPayload nettyPayload) {
        synchronized (lock) {
            queue.add(nettyPayload);
            currentSegmentBacklog++;
            if (nettyPayload.getSegmentId() == -1) {
                segmentBacklogQueue.add(currentSegmentBacklog);
                currentSegmentBacklog = 0;
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
            if (nettyPayload == null) {
                return null;
            }

            if (headSegmentBacklog > 0) {
                headSegmentBacklog--;
                if (headSegmentBacklog == 0) {
                    checkState(
                            nettyPayload.getSegmentId() == -1
                                    || nettyPayload.getError().isPresent());
                }
            } else {
                if (segmentBacklogQueue.isEmpty()) {
                    currentSegmentBacklog--;
                } else {
                    headSegmentBacklog = segmentBacklogQueue.poll();
                    headSegmentBacklog--;
                }
            }
            return nettyPayload;
        }
    }

    public int getBacklog() {
        synchronized (lock) {
            if (headSegmentBacklog > 0) {
                return headSegmentBacklog;
            }
            if (!segmentBacklogQueue.isEmpty()) {
                headSegmentBacklog = segmentBacklogQueue.poll();
                return headSegmentBacklog;
            }
            return currentSegmentBacklog;
        }
    }
}
