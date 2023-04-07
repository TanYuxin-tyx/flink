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

package org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.file;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierReader;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierReaderView;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierReaderViewId;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.RegionBufferIndexTracker;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Queue;
import java.util.function.Consumer;

/** The {@link ProducerMergePartitionTierReader} is used to consume shuffle data that's merged by producer. */
public interface ProducerMergePartitionTierReader extends Comparable<ProducerMergePartitionTierReader>, TierReader {

    /** Do prep work before this {@link ProducerMergePartitionTierReader} is scheduled to read data. */
    void prepareForScheduling();

    /** Read data from disk. */
    void readBuffers(Queue<MemorySegment> buffers, BufferRecycler recycler) throws IOException;

    /** Fail this {@link ProducerMergePartitionTierReader} caused by failureCause. */
    void fail(Throwable failureCause);

    /** Factory to create {@link ProducerMergePartitionTierReader}. */
    interface Factory {
        ProducerMergePartitionTierReader createFileReader(
                int subpartitionId,
                TierReaderViewId tierReaderViewId,
                FileChannel dataFileChannel,
                TierReaderView tierReaderView,
                RegionBufferIndexTracker dataIndex,
                int maxBuffersReadAhead,
                Consumer<ProducerMergePartitionTierReader> fileReaderReleaser,
                ByteBuffer headerBuffer);
    }
}