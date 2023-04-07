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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.file;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.NettyBasedTierConsumer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.NettyBasedTierConsumerView;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.TierReaderViewId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.tier.local.disk.RegionBufferIndexTracker;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Queue;
import java.util.function.Consumer;

/**
 * The {@link ProducerMergePartitionTierConsumer} is used to consume shuffle data that's merged by
 * producer.
 */
public interface ProducerMergePartitionTierConsumer
        extends Comparable<ProducerMergePartitionTierConsumer>, NettyBasedTierConsumer {

    /**
     * Do prep work before this {@link ProducerMergePartitionTierConsumer} is scheduled to read
     * data.
     */
    void prepareForScheduling();

    /** Read data from disk. */
    void readBuffers(Queue<MemorySegment> buffers, BufferRecycler recycler) throws IOException;

    /** Fail this {@link ProducerMergePartitionTierConsumer} caused by failureCause. */
    void fail(Throwable failureCause);

    /** Factory to create {@link ProducerMergePartitionTierConsumer}. */
    interface Factory {
        ProducerMergePartitionTierConsumer createFileReader(
                int subpartitionId,
                TierReaderViewId tierReaderViewId,
                FileChannel dataFileChannel,
                NettyBasedTierConsumerView tierConsumerView,
                RegionBufferIndexTracker dataIndex,
                int maxBuffersReadAhead,
                Consumer<ProducerMergePartitionTierConsumer> fileReaderReleaser,
                ByteBuffer headerBuffer);
    }
}
