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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.flink_31636;

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.CreditBasedShuffleViewId;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CreditBasedShuffleViewId}. */
class NettyServiceViewIdTest {

    @Test
    void testNewIdFromNull() {
        CreditBasedShuffleViewId creditBasedShuffleViewId =
                CreditBasedShuffleViewId.newId(null);
        assertThat(creditBasedShuffleViewId)
                .isNotNull()
                .isEqualTo(CreditBasedShuffleViewId.DEFAULT);
    }

    @Test
    void testConsumerIdEquals() {
        CreditBasedShuffleViewId creditBasedShuffleViewId =
                CreditBasedShuffleViewId.newId(null);
        CreditBasedShuffleViewId creditBasedShuffleViewId1 =
                CreditBasedShuffleViewId.newId(creditBasedShuffleViewId);
        CreditBasedShuffleViewId creditBasedShuffleViewId2 =
                CreditBasedShuffleViewId.newId(creditBasedShuffleViewId);
        assertThat(creditBasedShuffleViewId1.hashCode())
                .isEqualTo(creditBasedShuffleViewId2.hashCode());
        assertThat(creditBasedShuffleViewId1).isEqualTo(creditBasedShuffleViewId2);

        assertThat(CreditBasedShuffleViewId.newId(creditBasedShuffleViewId2))
                .isNotEqualTo(creditBasedShuffleViewId2);
    }
}
