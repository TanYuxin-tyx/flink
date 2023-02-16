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

package org.apache.flink.runtime.io.network.partition.store.tier.local.disk;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.partition.store.common.TierReader;
import org.apache.flink.runtime.io.network.partition.store.common.TierReaderViewId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Queue;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * This component is responsible for reading data from disk for a specific subpartition.
 *
 * <p>In order to access the disk as sequentially as possible {@link SubpartitionDiskReader} need to
 * be able to compare priorities.
 */
public interface SubpartitionDiskReader extends Comparable<SubpartitionDiskReader>, TierReader {
    /** Do prep work before this {@link SubpartitionDiskReader} is scheduled to read data. */
    void prepareForScheduling();

    /**
     * Read data from disk.
     *
     * @param buffers for reading, note that the ownership of the buffer taken out from the queue is
     *     transferred to this class, and the unused buffer must be returned.
     * @param recycler to return buffer to read buffer pool.
     */
    void readBuffers(Queue<MemorySegment> buffers, BufferRecycler recycler) throws IOException;

    /**
     * Fail this {@link SubpartitionDiskReader} caused by failureCause.
     *
     * @param failureCause represents the reason why it failed.
     */
    void fail(Throwable failureCause);

    void release();

    /** Factory to create {@link SubpartitionDiskReader}. */
    interface Factory {
        SubpartitionDiskReader createFileReader(
                int subpartitionId,
                TierReaderViewId tierReaderViewId,
                FileChannel dataFileChannel,
                SubpartitionDiskReaderViewOperations operation,
                RegionBufferIndexTracker dataIndex,
                int maxBuffersReadAhead,
                Consumer<SubpartitionDiskReader> fileReaderReleaser,
                BiFunction<Integer, Integer, Boolean> isLastRecordInSegmentDecider,
                ByteBuffer headerBuffer);
    }
}
