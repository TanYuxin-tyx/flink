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

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TieredStorageResourceRegistry}. */
class TieredStorageResourceRegistryTest {

    @Test
    void testAddLastReleasedResource() {
        int numNormalResources = 100;
        TieredStorageSubpartitionId subpartitionId = new TieredStorageSubpartitionId(0);
        TieredStorageResourceRegistry resourceRegistry = new TieredStorageResourceRegistry();

        AtomicInteger numReleasedForNormalResource = new AtomicInteger(0);
        AtomicInteger numReleasedForLastReleasedResource = new AtomicInteger(0);
        for (int i = 0; i < numNormalResources; i++) {
            resourceRegistry.registerResource(
                    subpartitionId,
                    () -> {
                        assertThat(numReleasedForLastReleasedResource.get() == 0).isTrue();
                        numReleasedForNormalResource.incrementAndGet();
                    });
            if (i == numNormalResources / 2) {
                resourceRegistry.registerLastReleasedResource(
                        subpartitionId, numReleasedForLastReleasedResource::incrementAndGet);
            }
        }
        resourceRegistry.clearResourceFor(subpartitionId);
    }
}
