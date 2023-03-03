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

import org.apache.flink.runtime.io.network.buffer.Buffer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.io.network.partition.tieredstore.upstream.TieredStoreTestUtils.createBuffer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

/** Tests for {@link BufferContext}. */
class BufferContextTest {
    private static final int BUFFER_SIZE = 16;

    private static final int SUBPARTITION_ID = 0;

    private static final int BUFFER_INDEX = 0;

    private BufferContext bufferContext;

    @BeforeEach
    void before() {
        bufferContext = createBufferContext();
    }

    @Test
    void testBufferStartSpillingRefCount() {
        Buffer buffer = bufferContext.getBuffer();
        CompletableFuture<Void> spilledFuture = new CompletableFuture<>();
        bufferContext.startSpilling();
        assertThat(bufferContext.isSpillStarted()).isTrue();
        assertThat(buffer.refCnt()).isEqualTo(2);
        spilledFuture.complete(null);
        assertThat(buffer.refCnt()).isEqualTo(1);
    }

    @Test
    void testBufferStartSpillingRepeatedly() {
        assertThat(bufferContext.startSpilling()).isTrue();
        assertThat(bufferContext.startSpilling()).isFalse();
    }

    @Test
    void testBufferReleaseRefCount() {
        Buffer buffer = bufferContext.getBuffer();
        assertThat(buffer.refCnt()).isEqualTo(1);
        bufferContext.release();
        assertThat(bufferContext.isReleased()).isTrue();
        assertThat(buffer.isRecycled()).isTrue();
    }

    @Test
    void testBufferReleaseRepeatedly() {
        bufferContext.release();
        assertThatNoException()
                .as("repeatedly release should only recycle buffer once.")
                .isThrownBy(() -> bufferContext.release());
    }

    @Test
    void testBufferConsumed() {
        final TierReaderViewId tierReaderViewId = TierReaderViewId.DEFAULT;
        Buffer buffer = bufferContext.getBuffer();
        //bufferContext.consumed(tierReaderViewId);
        assertThat(bufferContext.isConsumed(tierReaderViewId)).isTrue();
        assertThat(buffer.refCnt()).isEqualTo(2);
    }

    @Test
    void testBufferConsumedRepeatedly() {
        final TierReaderViewId tierReaderViewId = TierReaderViewId.DEFAULT;
        //bufferContext.consumed(tierReaderViewId);
        //assertThatThrownBy(() -> bufferContext.consumed(tierReaderViewId))
        //        .isInstanceOf(IllegalStateException.class)
        //        .hasMessageContaining("Consume buffer repeatedly is unexpected.");
    }

    @Test
    void testBufferConsumedMultipleConsumer() {
        TierReaderViewId consumer0 = TierReaderViewId.newId(null);
        TierReaderViewId consumer1 = TierReaderViewId.newId(consumer0);
        //bufferContext.consumed(consumer0);
        //bufferContext.consumed(consumer1);

        assertThat(bufferContext.isConsumed(consumer0)).isTrue();
        assertThat(bufferContext.isConsumed(consumer1)).isTrue();

        assertThat(bufferContext.isConsumed(TierReaderViewId.newId(consumer1))).isFalse();
    }

    @Test
    void testBufferStartSpillOrConsumedAfterReleased() {
        bufferContext.release();
        assertThat(bufferContext.startSpilling()).isFalse();
        //assertThatThrownBy(() -> bufferContext.consumed(TierReaderViewId.DEFAULT))
        //        .isInstanceOf(IllegalStateException.class)
        //        .hasMessageContaining("Buffer is already released.");
    }

    @Test
    void testBufferStartSpillingThenRelease() {
        Buffer buffer = bufferContext.getBuffer();
        CompletableFuture<Void> spilledFuture = new CompletableFuture<>();
        bufferContext.startSpilling();
        bufferContext.release();
        spilledFuture.complete(null);
        assertThat(buffer.isRecycled()).isTrue();
    }

    @Test
    void testBufferConsumedThenRelease() {
        Buffer buffer = bufferContext.getBuffer();
        //bufferContext.consumed(TierReaderViewId.DEFAULT);
        bufferContext.release();
        assertThat(buffer.refCnt()).isEqualTo(1);
    }

    private static BufferContext createBufferContext() {
        return new BufferContext(
                createBuffer(BUFFER_SIZE, false), BUFFER_INDEX, SUBPARTITION_ID, false);
    }
}
