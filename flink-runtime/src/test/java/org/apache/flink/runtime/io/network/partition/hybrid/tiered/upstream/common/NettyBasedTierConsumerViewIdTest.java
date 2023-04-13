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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common;

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.service.NettyBasedTierConsumerViewId;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link NettyBasedTierConsumerViewId}. */
class NettyBasedTierConsumerViewIdTest {

    @Test
    void testNewIdFromNull() {
        NettyBasedTierConsumerViewId nettyBasedTierConsumerViewId =
                NettyBasedTierConsumerViewId.newId(null);
        assertThat(nettyBasedTierConsumerViewId)
                .isNotNull()
                .isEqualTo(NettyBasedTierConsumerViewId.DEFAULT);
    }

    @Test
    void testConsumerIdEquals() {
        NettyBasedTierConsumerViewId nettyBasedTierConsumerViewId =
                NettyBasedTierConsumerViewId.newId(null);
        NettyBasedTierConsumerViewId nettyBasedTierConsumerViewId1 =
                NettyBasedTierConsumerViewId.newId(nettyBasedTierConsumerViewId);
        NettyBasedTierConsumerViewId nettyBasedTierConsumerViewId2 =
                NettyBasedTierConsumerViewId.newId(nettyBasedTierConsumerViewId);
        assertThat(nettyBasedTierConsumerViewId1.hashCode())
                .isEqualTo(nettyBasedTierConsumerViewId2.hashCode());
        assertThat(nettyBasedTierConsumerViewId1).isEqualTo(nettyBasedTierConsumerViewId2);

        assertThat(NettyBasedTierConsumerViewId.newId(nettyBasedTierConsumerViewId2))
                .isNotEqualTo(nettyBasedTierConsumerViewId2);
    }
}
