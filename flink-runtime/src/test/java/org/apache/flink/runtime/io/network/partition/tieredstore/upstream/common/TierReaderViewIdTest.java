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

package org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TierReaderViewId}. */
class TierReaderViewIdTest {
    @Test
    void testNewIdFromNull() {
        TierReaderViewId tierReaderViewId = TierReaderViewId.newId(null);
        assertThat(tierReaderViewId).isNotNull().isEqualTo(TierReaderViewId.DEFAULT);
    }

    @Test
    void testConsumerIdEquals() {
        TierReaderViewId tierReaderViewId = TierReaderViewId.newId(null);
        TierReaderViewId tierReaderViewId1 = TierReaderViewId.newId(tierReaderViewId);
        TierReaderViewId tierReaderViewId2 = TierReaderViewId.newId(tierReaderViewId);
        assertThat(tierReaderViewId1.hashCode()).isEqualTo(tierReaderViewId2.hashCode());
        assertThat(tierReaderViewId1).isEqualTo(tierReaderViewId2);

        assertThat(TierReaderViewId.newId(tierReaderViewId2)).isNotEqualTo(tierReaderViewId2);
    }
}
