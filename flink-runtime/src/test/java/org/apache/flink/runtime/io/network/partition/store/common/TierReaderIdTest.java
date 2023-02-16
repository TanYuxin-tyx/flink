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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TierReaderId}. */
class TierReaderIdTest {
    @Test
    void testNewIdFromNull() {
        TierReaderId tierReaderId = TierReaderId.newId(null);
        assertThat(tierReaderId).isNotNull().isEqualTo(TierReaderId.DEFAULT);
    }

    @Test
    void testConsumerIdEquals() {
        TierReaderId tierReaderId = TierReaderId.newId(null);
        TierReaderId tierReaderId1 = TierReaderId.newId(tierReaderId);
        TierReaderId tierReaderId2 = TierReaderId.newId(tierReaderId);
        assertThat(tierReaderId1.hashCode()).isEqualTo(tierReaderId2.hashCode());
        assertThat(tierReaderId1).isEqualTo(tierReaderId2);

        assertThat(TierReaderId.newId(tierReaderId2)).isNotEqualTo(tierReaderId2);
    }
}
