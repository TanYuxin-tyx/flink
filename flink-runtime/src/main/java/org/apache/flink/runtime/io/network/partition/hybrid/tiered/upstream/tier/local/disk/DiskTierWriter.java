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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.tier.local.disk;

import org.apache.flink.runtime.io.network.api.EndOfSegmentEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.SubpartitionSegmentIndexTracker;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.TierWriter;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.SEGMENT_EVENT;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** The DataManager of LOCAL file. */
public class DiskTierWriter implements TierWriter {

    private final int[] numSubpartitionEmitBytes;

    private final SubpartitionSegmentIndexTracker segmentIndexTracker;

    private final DiskCacheManager diskCacheManager;

    // TODO, Make this configurable.
    private int numBytesInASegment = 4 * 1024 * 1024; // 4 M

    private volatile boolean isClosed;

    public DiskTierWriter(
            int[] numSubpartitionEmitBytes,
            SubpartitionSegmentIndexTracker segmentIndexTracker,
            DiskCacheManager diskCacheManager) {
        this.numSubpartitionEmitBytes = numSubpartitionEmitBytes;
        this.segmentIndexTracker = segmentIndexTracker;
        this.diskCacheManager = diskCacheManager;
    }

    @Override
    public void setup() throws IOException {}

    @Override
    public void startSegment(int consumerId, int segmentId) {
        segmentIndexTracker.addSubpartitionSegmentIndex(consumerId, segmentId);
    }

    @Override
    public boolean write(int consumerId, Buffer finishedBuffer) throws IOException {
        boolean isLastBufferInSegment = false;
        numSubpartitionEmitBytes[consumerId] += finishedBuffer.readableBytes();
        if (numSubpartitionEmitBytes[consumerId] >= numBytesInASegment) {
            isLastBufferInSegment = true;
            numSubpartitionEmitBytes[consumerId] = 0;
        }
        if (isLastBufferInSegment) {
            emitBuffer(finishedBuffer, consumerId, false);
            emitEndOfSegmentEvent(consumerId);
        } else {
            emitBuffer(finishedBuffer, consumerId, isLastBufferInSegment);
        }
        return isLastBufferInSegment;
    }

    private void emitEndOfSegmentEvent(int targetChannel) {
        try {
            diskCacheManager.appendSegmentEvent(
                    EventSerializer.toSerializedEvent(EndOfSegmentEvent.INSTANCE),
                    targetChannel,
                    SEGMENT_EVENT);
        } catch (IOException e) {
            ExceptionUtils.rethrow(e, "Failed to emitEndOfSegmentEvent");
        }
    }

    private void emitBuffer(
            Buffer finishedBuffer, int targetSubpartition, boolean isLastBufferInSegment)
            throws IOException {
        diskCacheManager.append(finishedBuffer, targetSubpartition, isLastBufferInSegment);
    }

    @Override
    public void close() {
        if (!isClosed) {
            // close is called when task is finished or failed.
            checkNotNull(diskCacheManager).close();
            isClosed = true;
        }
    }
}
