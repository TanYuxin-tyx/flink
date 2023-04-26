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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage;

/** {@link SegmentSearcher} is used to search the specific segment id in each tier. */
public interface SegmentSearcher {

    /**
     * Query the provider for the existence of a segment.
     *
     * @param subpartitionId indicate the index of consumed subpartition.
     * @param segmentId indicate the id of segment.
     * @return whether the provider has the segment id.
     */
    boolean hasCurrentSegment(int subpartitionId, int segmentId);
}