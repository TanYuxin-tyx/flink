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

package org.apache.flink.runtime.io.network.partition.tieredstore.upstream;

/** Record the available writer and their segment index for single subpartition. */
public class WriterAndSegmentIndex {

    private final int writerIndex;

    private final boolean isLastBufferInSegment;

    private final int segmentIndex;

    private final int subpartitionId;

    public WriterAndSegmentIndex(
            int writerIndex, boolean isLastBufferInSegment, int segmentIndex, int subpartitionId) {
        this.writerIndex = writerIndex;
        this.isLastBufferInSegment = isLastBufferInSegment;
        this.segmentIndex = segmentIndex;
        this.subpartitionId = subpartitionId;
    }

    public int getWriterIndex() {
        return writerIndex;
    }

    public boolean isLastBufferInSegment() {
        return isLastBufferInSegment;
    }

    public long getSegmentIndex() {
        return segmentIndex;
    }

    public int getSubpartitionId() {
        return subpartitionId;
    }
}