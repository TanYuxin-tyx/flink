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

import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link HashBufferAccumulator}. */
class HashBufferAccumulatorTest {

    public static final int NUM_TOTAL_BUFFERS = 1000;

    public static final int NETWORK_BUFFER_SIZE = 1024;

    private static final float NUM_BUFFERS_TRIGGER_FLUSH_RATIO = 0.6f;

    private NetworkBufferPool globalPool;

    @BeforeEach
    void before() {
        globalPool = new NetworkBufferPool(NUM_TOTAL_BUFFERS, NETWORK_BUFFER_SIZE);
    }

    @AfterEach
    void after() {
        globalPool.destroy();
    }

    @Test
    void testAccumulateRecordsAndGenerateFinishedBuffers() throws IOException {
        int numBuffers = 10;
        int numRecords = 1000;
        TieredStorageSubpartitionId subpartitionId = new TieredStorageSubpartitionId(0);
        Random random = new Random();

        TieredStorageMemoryManager tieredStorageMemoryManager =
                createStorageMemoryManager(numBuffers);
        try (HashBufferAccumulator bufferAccumulator =
                new HashBufferAccumulator(1, NETWORK_BUFFER_SIZE, tieredStorageMemoryManager)) {
            AtomicInteger numReceivedFinishedBuffer = new AtomicInteger(0);
            bufferAccumulator.setup(
                    ((subpartition, buffers) ->
                            buffers.forEach(
                                    buffer -> {
                                        numReceivedFinishedBuffer.incrementAndGet();
                                        buffer.recycleBuffer();
                                    })));

            int numSentBytes = 0;
            for (int i = 0; i < numRecords; i++) {
                int numBytes = random.nextInt(2 * NETWORK_BUFFER_SIZE) + 1;
                numSentBytes += numBytes;
                ByteBuffer record = generateRandomData(numBytes, random);
                bufferAccumulator.receive(record, subpartitionId, Buffer.DataType.DATA_BUFFER);
            }
            ByteBuffer endEvent = EventSerializer.toSerializedEvent(EndOfPartitionEvent.INSTANCE);
            bufferAccumulator.receive(endEvent, subpartitionId, Buffer.DataType.EVENT_BUFFER);

            int numExpectBuffers =
                    numSentBytes / NETWORK_BUFFER_SIZE
                            + (numSentBytes % NETWORK_BUFFER_SIZE == 0 ? 0 : 1);

            assertThat(numReceivedFinishedBuffer.get()).isEqualTo(numExpectBuffers + 1);
        }
    }

    @Test
    void testEmitEventsBetweenRecords() throws IOException {
        int numBuffers = 10;
        int numRecords = 1000;
        TieredStorageSubpartitionId subpartitionId = new TieredStorageSubpartitionId(0);
        Random random = new Random();

        TieredStorageMemoryManager tieredStorageMemoryManager =
                createStorageMemoryManager(numBuffers);
        try (HashBufferAccumulator bufferAccumulator =
                new HashBufferAccumulator(1, NETWORK_BUFFER_SIZE, tieredStorageMemoryManager)) {
            AtomicInteger numReceivedFinishedBuffer = new AtomicInteger(0);
            bufferAccumulator.setup(
                    ((subpartition, buffers) ->
                            buffers.forEach(
                                    buffer -> {
                                        numReceivedFinishedBuffer.incrementAndGet();
                                        buffer.recycleBuffer();
                                    })));

            int numExpectBuffers = 0;
            for (int i = 0; i < numRecords; i++) {
                ByteBuffer byteBuffer;
                Buffer.DataType dataType;
                if (i % 2 == 0) {
                    // Need 3 network buffer to store this large record.
                    byteBuffer = generateRandomData(NETWORK_BUFFER_SIZE * 2 + 1, random);
                    dataType = Buffer.DataType.DATA_BUFFER;
                    numExpectBuffers += 3;
                } else {
                    byteBuffer = EventSerializer.toSerializedEvent(EndOfPartitionEvent.INSTANCE);
                    dataType = Buffer.DataType.EVENT_BUFFER;
                    numExpectBuffers++;
                }
                bufferAccumulator.receive(byteBuffer, subpartitionId, dataType);
            }
            ByteBuffer endEvent = EventSerializer.toSerializedEvent(EndOfPartitionEvent.INSTANCE);
            bufferAccumulator.receive(endEvent, subpartitionId, Buffer.DataType.EVENT_BUFFER);

            assertThat(numReceivedFinishedBuffer.get()).isEqualTo(numExpectBuffers + 1);
        }
    }

    private TieredStorageMemoryManagerImpl createStorageMemoryManager(int numBuffersInBufferPool)
            throws IOException {
        BufferPool bufferPool =
                globalPool.createBufferPool(numBuffersInBufferPool, numBuffersInBufferPool);
        TieredStorageMemoryManagerImpl storageMemoryManager =
                new TieredStorageMemoryManagerImpl(NUM_BUFFERS_TRIGGER_FLUSH_RATIO, true);
        storageMemoryManager.setup(
                bufferPool, Collections.singletonList(new TieredStorageMemorySpec(this, 1)));
        return storageMemoryManager;
    }

    private static ByteBuffer generateRandomData(int dataSize, Random random) {
        byte[] dataWritten = new byte[dataSize];
        random.nextBytes(dataWritten);
        return ByteBuffer.wrap(dataWritten);
    }
}
