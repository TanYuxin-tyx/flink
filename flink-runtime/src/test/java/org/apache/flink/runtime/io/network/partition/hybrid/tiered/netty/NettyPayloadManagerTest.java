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

import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;

import org.junit.jupiter.api.Test;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.END_OF_SEGMENT;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link NettyPayloadManager}. */
class NettyPayloadManagerTest {

    @Test
    void test1() {
        NettyPayloadManager nettyPayloadManager = new NettyPayloadManager();
        nettyPayloadManager.add(NettyPayload.newSegment(0));
        assertThat(nettyPayloadManager.getBacklog()).isZero();
        for (int i = 0; i < 3; i++) {
            nettyPayloadManager.add(
                    NettyPayload.newBuffer(BufferBuilderTestUtils.buildSomeBuffer(2), 0, 0));
        }
        nettyPayloadManager.add(
                NettyPayload.newBuffer(
                        new NetworkBuffer(
                                MemorySegmentFactory.allocateUnpooledSegment(0),
                                FreeingBufferRecycler.INSTANCE,
                                END_OF_SEGMENT),
                        0,
                        0));

        nettyPayloadManager.add(NettyPayload.newSegment(1));
        for (int i = 0; i < 4; i++) {
            nettyPayloadManager.add(
                    NettyPayload.newBuffer(BufferBuilderTestUtils.buildSomeBuffer(2), 0, 0));
        }
        nettyPayloadManager.poll();
        nettyPayloadManager.poll();
        nettyPayloadManager.add(
                NettyPayload.newBuffer(
                        new NetworkBuffer(
                                MemorySegmentFactory.allocateUnpooledSegment(0),
                                FreeingBufferRecycler.INSTANCE,
                                END_OF_SEGMENT),
                        0,
                        0));

        for (int i = 2; i >= 0; i--) {
            assertThat(nettyPayloadManager.getBacklog()).isEqualTo(i);
            nettyPayloadManager.poll();
        }
        nettyPayloadManager.poll();
        for (int i = 4; i >= 0; i--) {
            System.out.println(i);
            assertThat(nettyPayloadManager.getBacklog()).isEqualTo(i);
            nettyPayloadManager.poll();
        }
    }
}
