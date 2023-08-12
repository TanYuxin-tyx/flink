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

import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link NettyPayloadQueue}. */
class NettyPayloadQueueTest {

    @Test
    void testCase1() {
        // Add & Poll Case1
        // Add segment ID 0
        // Add Buffer 0
        // Add Buffer 1
        // Add Buffer 2
        // Add segment ID 1
        // Add Buffer 3
        // Add segment ID 3
        // Add Buffer 4
        // Add Buffer 5
        // Add Buffer 6
        // Add Buffer 7
        NettyPayloadQueue queue = new NettyPayloadQueue();
        queue.add(NettyPayload.newSegment(0));
        addBuffer(queue, 0);
        addBuffer(queue, 1);
        addBuffer(queue, 2);
        queue.add(NettyPayload.newSegment(1));
        addBuffer(queue, 3);
        queue.add(NettyPayload.newSegment(3));
        addBuffer(queue, 4);
        addBuffer(queue, 5);
        addBuffer(queue, 6);
        addBuffer(queue, 7);
        // Tests
        queue.poll();
        assertThat(queue.getBacklog()).isEqualTo(4);
        queue.poll();
        assertThat(queue.getBacklog()).isEqualTo(3);
        queue.poll();
        assertThat(queue.getBacklog()).isEqualTo(2);
        queue.poll();
        assertThat(queue.getBacklog()).isEqualTo(1);
        queue.poll();
        assertThat(queue.getBacklog()).isEqualTo(1);
        queue.poll();
        assertThat(queue.getBacklog()).isEqualTo(4);
        queue.poll();
        assertThat(queue.getBacklog()).isEqualTo(4);
        queue.poll();
        assertThat(queue.getBacklog()).isEqualTo(3);
        queue.poll();
        assertThat(queue.getBacklog()).isEqualTo(2);
        queue.poll();
        assertThat(queue.getBacklog()).isEqualTo(1);
        queue.poll();
        assertThat(queue.getBacklog()).isEqualTo(0);
    }

    @Test
    void testCase2() {
        // Add & Poll Case1
        // Add segment ID 0
        // Pool and getBacklog
        // Add Buffer 0
        // Pool and getBacklog
        // Add Buffer 1
        // Add segment ID 1
        // Add Buffer 2
        // Pool and getBacklog
        NettyPayloadQueue queue = new NettyPayloadQueue();
        queue.add(NettyPayload.newSegment(0));
        queue.poll();
        assertThat(queue.getBacklog()).isEqualTo(0);
        addBuffer(queue, 0);
        queue.poll();
        assertThat(queue.getBacklog()).isEqualTo(0);
        addBuffer(queue, 1);
        queue.add(NettyPayload.newSegment(1));
        addBuffer(queue, 2);
        assertThat(queue.getBacklog()).isEqualTo(2);
        queue.poll();
        assertThat(queue.getBacklog()).isEqualTo(1);
    }

    private void addBuffer(NettyPayloadQueue queue, int bufferIndex) {
        queue.add(
                NettyPayload.newBuffer(BufferBuilderTestUtils.buildSomeBuffer(0), bufferIndex, 0));
    }
}
