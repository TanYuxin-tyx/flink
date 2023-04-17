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

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.shuffle.TierType;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.common.TierStorage;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.common.TierProducerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.common.TieredStorageWriterFactory;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;

/** The DataManager of DFS. */
public class RemoteTierStorage implements TierStorage {

    private final TieredStorageWriterFactory tieredStorageWriterFactory;

    private TierProducerAgent tierProducerAgent;

    public RemoteTierStorage(TieredStorageWriterFactory tieredStorageWriterFactory) {
        this.tieredStorageWriterFactory = tieredStorageWriterFactory;
    }

    @Override
    public void setup() throws IOException {}

    @Override
    public TierProducerAgent createTierStorageWriter() {
        try {
            tierProducerAgent =
                    tieredStorageWriterFactory.createTierStorageWriter(TierType.IN_REMOTE);
        } catch (IOException e) {
            ExceptionUtils.rethrow(e, "Failed to craete remote tier writer");
        }
        return tierProducerAgent;
    }

    @Override
    public boolean canStoreNextSegment(int consumerId) {
        return true;
    }

    @Override
    public TierType getTierType() {
        return TierType.IN_REMOTE;
    }

    @Override
    public void release() {
        ((RemoteTierProducerAgent) tierProducerAgent).getRemoteCacheManager().release();
        ((RemoteTierProducerAgent) tierProducerAgent).getSegmentIndexTracker().release();
    }
}
