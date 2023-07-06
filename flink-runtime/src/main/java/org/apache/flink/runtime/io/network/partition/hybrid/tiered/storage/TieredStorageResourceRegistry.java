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

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageDataIdentifier;

import java.util.Deque;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A registry that maintains local or remote resources that correspond to a certain set of data in
 * the Tiered Storage.
 */
public class TieredStorageResourceRegistry {

    private final Map<TieredStorageDataIdentifier, Deque<TieredStorageResource>>
            registeredResources = new ConcurrentHashMap<>();

    /**
     * Register a new resource for the given owner.
     *
     * @param owner identifier of the data that the resource corresponds to.
     * @param tieredResource the tiered storage resources to be registered.
     */
    public void registerResource(
            TieredStorageDataIdentifier owner, TieredStorageResource tieredResource) {
        registeredResources
                .computeIfAbsent(owner, (ignore) -> new LinkedList<>())
                .addFirst(tieredResource);
    }

    /**
     * Register a new resource which will be released at the last order for the given owner.
     *
     * @param owner identifier of the data that the resource corresponds to.
     * @param tieredResource the tiered storage resources to be registered.
     */
    public void registerLastReleasedResource(
            TieredStorageDataIdentifier owner, TieredStorageResource tieredResource) {
        registeredResources
                .computeIfAbsent(owner, (ignore) -> new LinkedList<>())
                .addLast(tieredResource);
    }

    /**
     * Remove all resources for the given owner.
     *
     * @param owner identifier of the data that the resources correspond to.
     */
    public void clearResourceFor(TieredStorageDataIdentifier owner) {
        Deque<TieredStorageResource> cleanersForOwner = registeredResources.remove(owner);

        while (cleanersForOwner != null && !cleanersForOwner.isEmpty()) {
            cleanersForOwner.pollFirst().release();
        }
    }
}
