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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.service;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * {@link NettyBasedTierConsumerViewId} represents the identifier of {@link
 * NettyServiceView}, Every {@link NettyServiceView} has a specific id.
 */
public class NettyBasedTierConsumerViewId {

    /** This is the first consumer view id of a single subpartition. */
    public static final NettyBasedTierConsumerViewId DEFAULT = new NettyBasedTierConsumerViewId(0);

    /** This is a unique field for each consumer view of a single subpartition. */
    private final int id;

    private NettyBasedTierConsumerViewId(int id) {
        this.id = id;
    }

    public static NettyBasedTierConsumerViewId newId(
            @Nullable NettyBasedTierConsumerViewId lastId) {
        return lastId == null ? DEFAULT : new NettyBasedTierConsumerViewId(lastId.id + 1);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NettyBasedTierConsumerViewId that = (NettyBasedTierConsumerViewId) o;
        return id == that.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
