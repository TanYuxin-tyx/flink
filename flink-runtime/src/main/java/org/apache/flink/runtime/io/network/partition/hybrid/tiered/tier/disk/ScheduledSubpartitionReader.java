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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.disk;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionWriter;

import java.io.IOException;
import java.util.Queue;

/**
 * {@link ScheduledSubpartitionReader} is the reader of a subpartition, which is scheduled by {@link
 * DiskIOScheduler} and read data from disk.
 */
public interface ScheduledSubpartitionReader extends Comparable<ScheduledSubpartitionReader> {

    /**
     * Load data from disk to buffers, which will be written to {@link NettyConnectionWriter}.
     *
     * @param buffers available buffers to load data.
     * @param recycler the recycler to recycle buffers.
     * @throws IOException exception will be thrown if an error is happened.
     */
    void loadDiskDataToBuffers(Queue<MemorySegment> buffers, BufferRecycler recycler)
            throws IOException;

    /**
     * Get the current reading file offset, which will be used to decide the execution order between
     * different {@link ScheduledSubpartitionReader}s and achieve sequential reading.
     *
     * @return the file offset.
     */
    long getReadingFileOffset();

    /**
     * {@link DiskIOScheduler} will fail the {@link ScheduledSubpartitionReader} if an error is
     * happened during scheduling.
     *
     * @param failureCause the error.
     */
    void failReader(Throwable failureCause);
}
