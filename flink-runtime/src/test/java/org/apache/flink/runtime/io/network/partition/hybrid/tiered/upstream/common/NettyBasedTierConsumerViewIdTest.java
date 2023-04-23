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

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.service.NettyServiceViewId;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link NettyServiceViewId}. */
class NettyBasedTierConsumerViewIdTest {

    @Test
    void testNewIdFromNull() {
        NettyServiceViewId nettyServiceViewId =
                NettyServiceViewId.newId(null);
        assertThat(nettyServiceViewId)
                .isNotNull()
                .isEqualTo(NettyServiceViewId.DEFAULT);
    }

    @Test
    void testConsumerIdEquals() {
        NettyServiceViewId nettyServiceViewId =
                NettyServiceViewId.newId(null);
        NettyServiceViewId nettyServiceViewId1 =
                NettyServiceViewId.newId(nettyServiceViewId);
        NettyServiceViewId nettyServiceViewId2 =
                NettyServiceViewId.newId(nettyServiceViewId);
        assertThat(nettyServiceViewId1.hashCode())
                .isEqualTo(nettyServiceViewId2.hashCode());
        assertThat(nettyServiceViewId1).isEqualTo(nettyServiceViewId2);

        assertThat(NettyServiceViewId.newId(nettyServiceViewId2))
                .isNotEqualTo(nettyServiceViewId2);
    }
}
