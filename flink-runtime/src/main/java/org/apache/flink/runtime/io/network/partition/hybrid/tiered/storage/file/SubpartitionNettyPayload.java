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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file;

import java.util.List;

/**
 * The wrapper class {@link SubpartitionNettyPayload} for a subpartition, which holds all the {@link
 * SegmentNettyPayload} buffers.
 */
public class SubpartitionNettyPayload {

    private final int subpartitionId;

    private final List<SegmentNettyPayload> segmentNettyPayloads;

    public SubpartitionNettyPayload(
            int subpartitionId, List<SegmentNettyPayload> segmentNettyPayloads) {
        this.subpartitionId = subpartitionId;
        this.segmentNettyPayloads = segmentNettyPayloads;
    }

    int getSubpartitionId() {
        return subpartitionId;
    }

    List<SegmentNettyPayload> getSegmentNettyPayloads() {
        return segmentNettyPayloads;
    }
}
