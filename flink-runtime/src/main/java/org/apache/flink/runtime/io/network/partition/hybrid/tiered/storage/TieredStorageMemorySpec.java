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

/**
 * The memory specs for a memory owner, including the owner itself, the number of exclusive buffers
 * of the owner, whether the owner's memory can be released when not consumed, etc.
 */
public class TieredStorageMemorySpec {

    /** The memory use owner. */
    private final Object owner;

    private final int numGuaranteedBuffers;

    /**
     * The memory managed by {@link TieredStorageMemoryManager} is categorized into two types:
     * long-term occupied memory which cannot be immediately released and short-term occupied memory
     * which can be reclaimed quickly and safely. Long-term occupied memory usage necessitates
     * waiting for other operations to complete before releasing it, such as downstream consumption.
     * On the other hand, short-term occupied memory can be freed up at any time, enabling rapid
     * memory recycling for tasks such as flushing memory to disk or remote storage.
     *
     * <p>This field is to indicate whether the tiered storage memory is releasable. If a user is a
     * long-term occupied memory user, this field is false, while if a user is a short-term occupied
     * memory user, this field is true.
     */
    private final boolean isMemoryReleasable;

    public TieredStorageMemorySpec(
            Object owner, int numGuaranteedBuffers, boolean isMemoryReleasable) {
        this.owner = owner;
        this.numGuaranteedBuffers = numGuaranteedBuffers;
        this.isMemoryReleasable = isMemoryReleasable;
    }

    public Object getOwner() {
        return owner;
    }

    public int getNumGuaranteedBuffers() {
        return numGuaranteedBuffers;
    }

    public boolean isMemoryReleasable() {
        return isMemoryReleasable;
    }
}
