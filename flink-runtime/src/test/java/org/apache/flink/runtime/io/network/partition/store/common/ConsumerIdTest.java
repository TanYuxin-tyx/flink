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

/** Tests for {@link ConsumerId}. */
class ConsumerIdTest {
    @Test
    void testNewIdFromNull() {
        ConsumerId consumerId = ConsumerId.newId(null);
        assertThat(consumerId).isNotNull().isEqualTo(ConsumerId.DEFAULT);
    }

    @Test
    void testConsumerIdEquals() {
        ConsumerId consumerId = ConsumerId.newId(null);
        ConsumerId consumerId1 = ConsumerId.newId(consumerId);
        ConsumerId consumerId2 = ConsumerId.newId(consumerId);
        assertThat(consumerId1.hashCode()).isEqualTo(consumerId2.hashCode());
        assertThat(consumerId1).isEqualTo(consumerId2);

        assertThat(ConsumerId.newId(consumerId2)).isNotEqualTo(consumerId2);
    }
}
