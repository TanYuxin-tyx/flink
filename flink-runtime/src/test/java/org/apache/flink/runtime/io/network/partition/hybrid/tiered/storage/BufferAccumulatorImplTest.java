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
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageTestUtils.generateRandomData;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BufferAccumulatorImpl}. */
class BufferAccumulatorImplTest {

    private NetworkBufferPool globalPool;

    @BeforeEach
    void before() {
        globalPool =
                new NetworkBufferPool(
                        TieredStorageTestUtils.NUM_TOTAL_BUFFERS,
                        TieredStorageTestUtils.NETWORK_BUFFER_SIZE);
    }

    @AfterEach
    void after() {
        globalPool.destroy();
    }

    @Test
    void testAccumulateRecordsAndGenerateFinishedBuffers() throws IOException {
        int numBuffers = 10;
        int numRecords = 1000;
        Random random = new Random();

        TieredStorageMemoryManager tieredStorageMemoryManager =
                createStorageMemoryManager(numBuffers);
        BufferAccumulatorImpl bufferAccumulator =
                new BufferAccumulatorImpl(
                        TieredStorageTestUtils.NETWORK_BUFFER_SIZE, 1, tieredStorageMemoryManager);

        AtomicInteger numReceivedFinishedBuffer = new AtomicInteger(0);

        int numSentBytes = 0;
        for (int i = 0; i < numRecords; i++) {
            int numBytes = random.nextInt(2 * TieredStorageTestUtils.NETWORK_BUFFER_SIZE) + 1;
            numSentBytes += numBytes;
            ByteBuffer record = generateRandomData(numBytes, random);
            bufferAccumulator.receive(
                    record, new TieredStorageSubpartitionId(0), Buffer.DataType.DATA_BUFFER);
        }
        ByteBuffer endEvent = EventSerializer.toSerializedEvent(EndOfPartitionEvent.INSTANCE);
        bufferAccumulator.receive(
                endEvent, new TieredStorageSubpartitionId(0), Buffer.DataType.EVENT_BUFFER);

        int numExpectBuffers =
                numSentBytes / TieredStorageTestUtils.NETWORK_BUFFER_SIZE
                        + (numSentBytes % TieredStorageTestUtils.NETWORK_BUFFER_SIZE == 0 ? 0 : 1);

        assertThat(numReceivedFinishedBuffer.get()).isEqualTo(numExpectBuffers + 1);
        bufferAccumulator.close();
    }

    @Test
    void testEmitEventsBetweenRecords() throws IOException {
        int numBuffers = 10;
        int numRecords = 1000;
        Random random = new Random();
        TieredStorageMemoryManager tieredStorageMemoryManager =
                createStorageMemoryManager(numBuffers);
        BufferAccumulatorImpl bufferAccumulator =
                new BufferAccumulatorImpl(
                        TieredStorageTestUtils.NETWORK_BUFFER_SIZE, 1, tieredStorageMemoryManager);

        for (int i = 0; i < numRecords; i++) {
            if (i % 2 == 0) {
                // Need 3 network buffer to store this large record.
                ByteBuffer record =
                        generateRandomData(
                                TieredStorageTestUtils.NETWORK_BUFFER_SIZE * 2 + 1, random);
                bufferAccumulator.receive(
                        record, new TieredStorageSubpartitionId(0), Buffer.DataType.DATA_BUFFER);
            } else {
                ByteBuffer endEvent =
                        EventSerializer.toSerializedEvent(EndOfPartitionEvent.INSTANCE);
                bufferAccumulator.receive(
                        endEvent, new TieredStorageSubpartitionId(0), Buffer.DataType.EVENT_BUFFER);
            }
        }
        ByteBuffer endEvent = EventSerializer.toSerializedEvent(EndOfPartitionEvent.INSTANCE);
        bufferAccumulator.receive(
                endEvent, new TieredStorageSubpartitionId(0), Buffer.DataType.EVENT_BUFFER);

        bufferAccumulator.close();
    }

    private TieredStorageMemoryManagerImpl createStorageMemoryManager(int numBuffersInBufferPool)
            throws IOException {
        BufferPool bufferPool =
                globalPool.createBufferPool(numBuffersInBufferPool, numBuffersInBufferPool);
        TieredStorageMemoryManagerImpl storageMemoryManager = new TieredStorageMemoryManagerImpl();
        storageMemoryManager.setup(bufferPool);
        return storageMemoryManager;
    }
}
