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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered;

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.TierStorage;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.TierStorageFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.tier.remote.RemoteTierStorageFactory;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;

public class RemoteTieredStorageFactory {

    private final TieredStorageWriterFactory tieredStorageWriterFactory;

    private final TierStorage[] tierStorages;

    private final TierType[] tierTypes;

    public RemoteTieredStorageFactory(
            TierType[] tierTypes, TieredStorageWriterFactory tieredStorageWriterFactory) {
        this.tierTypes = tierTypes;
        this.tierStorages = new TierStorage[tierTypes.length];
        this.tieredStorageWriterFactory = tieredStorageWriterFactory;
    }

    public void setup() {
        try {
            for (int i = 0; i < tierTypes.length; i++) {
                tierStorages[i] = createTierStorage(tierTypes[i]);
            }
        } catch (IOException e) {
            ExceptionUtils.rethrow(e);
        }
    }

    public TierStorage[] getTierStorages() {
        return tierStorages;
    }

    private TierStorage createTierStorage(TierType tierType) throws IOException {
        TierStorageFactory tierStorageFactory;
        switch (tierType) {
            case IN_REMOTE:
                tierStorageFactory = getRemoteTierStorageFactory();
                break;
            default:
                throw new IllegalArgumentException("Illegal tier type " + tierType);
        }
        TierStorage tierStorage = tierStorageFactory.createTierStorage();
        tierStorage.setup();
        return tierStorage;
    }

    private TierStorageFactory getRemoteTierStorageFactory() {
        return new RemoteTierStorageFactory(tieredStorageWriterFactory);
    }
}
