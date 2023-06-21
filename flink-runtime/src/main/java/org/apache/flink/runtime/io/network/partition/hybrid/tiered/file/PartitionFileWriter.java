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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.file;

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * The {@link PartitionFileWriter} interface defines the write logic for different types of shuffle
 * files.
 */
public interface PartitionFileWriter {

    /**
     * Write the {@link SpilledBufferContext}s to the partition file. The written buffers may belong
     * to multiple subpartitions.
     *
     * @param partitionId the partition id
     * @param spilledBuffers the buffers to be flushed
     * @return the completable future indicating whether the writing file process has finished. If
     *     the {@link CompletableFuture} is completed, the written process is completed.
     */
    CompletableFuture<Void> write(
            TieredStoragePartitionId partitionId,
            List<SubpartitionSpilledBufferContext> spilledBuffers);

    /** Release all the resources of the {@link PartitionFileWriter}. */
    void release();

    /**
     * The {@link SubpartitionSpilledBufferContext} contains all the buffers that will be spilled in
     * this subpartition.
     */
    class SubpartitionSpilledBufferContext {

        /** The subpartition id. */
        private final int subpartitionId;

        /** he {@link SegmentSpilledBufferContext}s belonging to this subpartition. */
        private final List<SegmentSpilledBufferContext> segmentSpilledBufferContexts;

        public SubpartitionSpilledBufferContext(
                int subpartitionId,
                List<SegmentSpilledBufferContext> segmentSpilledBufferContexts) {
            this.subpartitionId = subpartitionId;
            this.segmentSpilledBufferContexts = segmentSpilledBufferContexts;
        }

        public int getSubpartitionId() {
            return subpartitionId;
        }

        public List<SegmentSpilledBufferContext> getSegmentSpillBufferContexts() {
            return segmentSpilledBufferContexts;
        }
    }

    /**
     * The wrapper class {@link SegmentSpilledBufferContext} contains all the {@link
     * SpilledBufferContext}s of the segment. Note that when this indicates the segment need to be
     * finished, the field {@code spilledBufferContexts} should be empty.
     */
    class SegmentSpilledBufferContext {

        /** The segment id. */
        private final int segmentId;

        /** The {@link SpilledBufferContext}s indicate the buffers to be spilled. */
        private final List<SpilledBufferContext> spilledBufferContexts;

        /** Whether it is necessary to finish the segment. */
        private final boolean needFinishSegment;

        public SegmentSpilledBufferContext(
                int segmentId,
                List<SpilledBufferContext> spilledBufferContexts,
                boolean needFinishSegment) {
            this.segmentId = segmentId;
            this.spilledBufferContexts = spilledBufferContexts;
            this.needFinishSegment = needFinishSegment;
        }

        public int getSegmentId() {
            return segmentId;
        }

        public List<SpilledBufferContext> getSpillBufferContexts() {
            return spilledBufferContexts;
        }

        public boolean needFinishSegment() {
            return needFinishSegment;
        }
    }
}
