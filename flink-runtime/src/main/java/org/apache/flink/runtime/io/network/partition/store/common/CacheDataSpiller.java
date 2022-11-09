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

package org.apache.flink.runtime.io.network.partition.store.common;

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.io.network.partition.store.tier.local.file.RegionBufferIndexTracker;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** Spilling the caching data in a cached data manager. */
public interface CacheDataSpiller {

    void startSegment(long segmentIndex) throws IOException;

    CompletableFuture<List<RegionBufferIndexTracker.SpilledBuffer>> spillAsync(
            List<BufferWithIdentity> bufferToSpill);

    void finishSegment(long segmentIndex);

    void release();

    void close();

    Path getBaseSubpartitionPath();
}
