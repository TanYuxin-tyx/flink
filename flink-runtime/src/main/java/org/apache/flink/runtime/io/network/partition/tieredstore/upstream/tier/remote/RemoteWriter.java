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

package org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.remote;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.SubpartitionSegmentIndexTracker;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Through the {@link RemoteWriter}, records from {@link RemoteTier} is writen to cached buffers.
 */
public class RemoteWriter implements TierWriter {

    public static final int BROADCAST_CHANNEL = 0;

    private final int numSubpartitions;

    private final boolean isBroadcastOnly;

    // Record the byte number currently written to each sub partition.
    private final int[] numSubpartitionEmitBytes;

    private final SubpartitionSegmentIndexTracker segmentIndexTracker;

    private final RemoteCacheManager cacheDataManager;

    private final int numBytesInASegment;

    public RemoteWriter(
            int numSubpartitions,
            boolean isBroadcastOnly,
            SubpartitionSegmentIndexTracker segmentIndexTracker,
            RemoteCacheManager remoteCacheManager,
            int numBytesInASegment) {
        this.numSubpartitions = numSubpartitions;
        this.isBroadcastOnly = isBroadcastOnly;
        this.segmentIndexTracker = segmentIndexTracker;
        this.cacheDataManager = remoteCacheManager;
        this.numSubpartitionEmitBytes = new int[numSubpartitions];
        Arrays.fill(numSubpartitionEmitBytes, 0);
        this.numBytesInASegment = numBytesInASegment;
    }

    @Override
    public void setup() throws IOException {}

    @Override
    public boolean emit(
            ByteBuffer record,
            int targetSubpartition,
            Buffer.DataType dataType,
            boolean isBroadcast,
            boolean isEndOfPartition,
            long segmentIndex)
            throws IOException {

        boolean isLastRecordInSegment = false;
        numSubpartitionEmitBytes[targetSubpartition] += record.remaining();
        if (numSubpartitionEmitBytes[targetSubpartition] >= numBytesInASegment) {
            isLastRecordInSegment = true;
            numSubpartitionEmitBytes[targetSubpartition] = 0;
        }

        if (!segmentIndexTracker.hasCurrentSegment(targetSubpartition, segmentIndex)) {
            cacheDataManager.startSegment(targetSubpartition, segmentIndex);
        }
        emit(record, targetSubpartition, dataType, isLastRecordInSegment);
        if (isLastRecordInSegment || isEndOfPartition) {
            cacheDataManager.finishSegment(targetSubpartition, segmentIndex);
            segmentIndexTracker.addSubpartitionSegmentIndex(targetSubpartition, segmentIndex);
        }

        return isLastRecordInSegment;
    }

    private void emit(
            ByteBuffer record,
            int targetSubpartition,
            Buffer.DataType dataType,
            boolean isLastRecordInSegment)
            throws IOException {
        cacheDataManager.append(record, targetSubpartition, dataType, isLastRecordInSegment);
    }

    @Override
    public void release() {
        cacheDataManager.release();
    }

    @Override
    public void close() {
        cacheDataManager.close();
    }
}
