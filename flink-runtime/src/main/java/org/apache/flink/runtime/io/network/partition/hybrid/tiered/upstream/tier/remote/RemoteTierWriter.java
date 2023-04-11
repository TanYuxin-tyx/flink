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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.tier.remote;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.SubpartitionSegmentIndexTracker;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.TierWriter;

import java.io.IOException;
import java.util.Arrays;

/**
 * Through the {@link RemoteTierWriter}, records from {@link RemoteTierStorage} is written to cached
 * buffers.
 */
public class RemoteTierWriter implements TierWriter {

    // Record the byte number currently written to each sub partition.
    private final int[] numSubpartitionEmitBytes;

    private final SubpartitionSegmentIndexTracker segmentIndexTracker;

    private final RemoteCacheManager cacheDataManager;

    private int numBytesInASegment;

    private final int[] subpartitionLastestSegmentId;

    public RemoteTierWriter(
            int numSubpartitions,
            SubpartitionSegmentIndexTracker segmentIndexTracker,
            RemoteCacheManager remoteCacheManager,
            int numBytesInASegment) {
        this.segmentIndexTracker = segmentIndexTracker;
        this.cacheDataManager = remoteCacheManager;
        this.numSubpartitionEmitBytes = new int[numSubpartitions];
        this.subpartitionLastestSegmentId = new int[numSubpartitions];
        Arrays.fill(numSubpartitionEmitBytes, 0);
        this.numBytesInASegment = numBytesInASegment;
    }

    @Override
    public void setup() throws IOException {}

    @Override
    public void startSegment(int consumerId, int segmentId) {
        if (!segmentIndexTracker.hasCurrentSegment(consumerId, segmentId)) {
            segmentIndexTracker.addSubpartitionSegmentIndex(consumerId, segmentId);
            cacheDataManager.startSegment(consumerId, segmentId);
        }
        subpartitionLastestSegmentId[consumerId] = segmentId;
    }

    @Override
    public boolean emit(int consumerId, Buffer finishedBuffer)
            throws IOException {
        boolean isLastBufferInSegment = false;
        numSubpartitionEmitBytes[consumerId] += finishedBuffer.readableBytes();
        if (numSubpartitionEmitBytes[consumerId] >= numBytesInASegment) {
            isLastBufferInSegment = true;
            numSubpartitionEmitBytes[consumerId] = 0;
        }
        emitBuffer(finishedBuffer, consumerId);

        if (isLastBufferInSegment) {
            cacheDataManager.finishSegment(
                    consumerId, subpartitionLastestSegmentId[consumerId]);
        }
        return isLastBufferInSegment;
    }

    private void emitBuffer(Buffer finishedBuffer, int targetSubpartition) {
        cacheDataManager.appendBuffer(finishedBuffer, targetSubpartition);
    }

    @Override
    public void close() {
        for (int index = 0; index < subpartitionLastestSegmentId.length; ++index) {
            cacheDataManager.finishSegment(index, subpartitionLastestSegmentId[index]);
        }
        cacheDataManager.close();
    }
}
