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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.remote;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.BufferContext;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file.PartitionFileWriter;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkState;

/** This class is responsible for managing the data in a single subpartition. */
public class SubpartitionRemoteCacheManager {

    private static final Logger LOG = LoggerFactory.getLogger(SubpartitionRemoteCacheManager.class);

    private final int targetChannel;

    // Not guarded by lock because it is expected only accessed from task's main thread.
    private final Queue<BufferBuilder> unfinishedBuffers = new LinkedList<>();

    // Not guarded by lock because it is expected only accessed from task's main thread.
    private int finishedBufferIndex;

    private final Deque<BufferContext> allBuffers = new LinkedList<>();

    private final PartitionFileWriter partitionFileWriter;

    private final AtomicInteger currentSegmentId = new AtomicInteger(-1);

    private volatile boolean isReleased;

    private CompletableFuture<Void> lastSpillFuture = CompletableFuture.completedFuture(null);

    public SubpartitionRemoteCacheManager(
            int targetChannel,
            TieredStorageMemoryManager storageMemoryManager,
            PartitionFileWriter partitionFileWriter) {
        this.targetChannel = targetChannel;
        storageMemoryManager.registerCacheBufferFlushTrigger(this::flushCachedBuffers);
        this.partitionFileWriter = partitionFileWriter;
    }

    // ------------------------------------------------------------------------
    //  Called by DfsCacheDataManager
    // ------------------------------------------------------------------------

    public void startSegment(int segmentIndex) {
        checkState(currentSegmentId.get() != segmentIndex);
        synchronized (currentSegmentId) {
            currentSegmentId.set(segmentIndex);
        }
    }

    public void finishSegment(int segmentIndex) {
        checkState(currentSegmentId.get() == -1 || currentSegmentId.get() == segmentIndex);
        int bufferNumber = flushCachedBuffers();
        if (bufferNumber > 0) {
            lastSpillFuture = partitionFileWriter.finishSegment(targetChannel, segmentIndex);
        }
        checkState(allBuffers.isEmpty(), "Leaking finished buffers.");
    }

    /** Release all buffers. */
    public void release() {
        try {
            lastSpillFuture.get();
        } catch (Exception e) {
            LOG.error("Failed to finish the spilling process.", e);
            ExceptionUtils.rethrow(e);
        }
        if (!isReleased) {
            for (BufferContext bufferContext : allBuffers) {
                Buffer buffer = bufferContext.getBuffer();
                if (!buffer.isRecycled()) {
                    buffer.recycleBuffer();
                }
            }
            allBuffers.clear();
            isReleased = true;
        }
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    void addFinishedBuffer(Buffer buffer) {
        BufferContext toAddBuffer = new BufferContext(buffer, finishedBufferIndex, targetChannel);
        addFinishedBuffer(toAddBuffer);
    }

    @SuppressWarnings("FieldAccessNotGuarded")
    // Note that: callWithLock ensure that code block guarded by resultPartitionReadLock and
    // subpartitionLock.
    private void addFinishedBuffer(BufferContext bufferContext) {
        synchronized (allBuffers) {
            finishedBufferIndex++;
            allBuffers.add(bufferContext);
        }
    }

    private int flushCachedBuffers() {
        List<BufferContext> bufferContexts = generateToSpillBuffersWithId();
        if (bufferContexts.size() > 0) {
            synchronized (currentSegmentId) {
                lastSpillFuture =
                        partitionFileWriter.spillAsync(
                                targetChannel, currentSegmentId.get(), bufferContexts);
            }
        }
        return bufferContexts.size();
    }

    private List<BufferContext> generateToSpillBuffersWithId() {
        synchronized (allBuffers) {
            List<BufferContext> targetBuffers = new ArrayList<>(allBuffers);
            allBuffers.clear();
            return targetBuffers;
        }
    }

    void close() {
        try {
            lastSpillFuture.get();
        } catch (Exception e) {
            LOG.error("Failed to finish the spilling process.", e);
            ExceptionUtils.rethrow(e);
        }
        flushCachedBuffers();
        while (!unfinishedBuffers.isEmpty()) {
            unfinishedBuffers.poll().close();
        }
    }

    @VisibleForTesting
    public Path getBaseSubpartitionPath() {
        return null;
    }
}
