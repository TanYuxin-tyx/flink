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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.partition.BufferWithChannel;
import org.apache.flink.runtime.io.network.partition.DataBuffer;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.TestingTieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageIdMappingUtils;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Random;

/** Tests for {@link SortBufferAccumulator}. */
public class SortBufferAccumulatorTest {

    @Test
    public void testWriteAndReadDataBuffer() throws Exception {
        int numSubpartitions = 10;
        int bufferSize = 1024;
        int bufferPoolSize = 512;
        Random random = new Random(1111);
        TieredStoragePartitionId partitionId =
                TieredStorageIdMappingUtils.convertId(new ResultPartitionID());
        TieredStorageSubpartitionId subpartitionId = new TieredStorageSubpartitionId(0);
        TieredStorageMemoryManager memoryManager =
                new TestingTieredStorageMemoryManager.Builder()
                        .setRequestBufferBlockingFunction(
                                obj ->
                                        new BufferBuilder(
                                                MemorySegmentFactory.allocateUnpooledSegment(
                                                        bufferSize),
                                                SortBufferAccumulatorTest.this::recycleBuffer))
                        .build();

        List<Buffer> accumulatedBuffers = new ArrayList<>();
        SortBufferAccumulator sortBufferAccumulator =
                new SortBufferAccumulator(
                        partitionId,
                        numSubpartitions,
                        1,
                        bufferSize,
                        memoryManager,
                        new TieredStorageResourceRegistry());
        sortBufferAccumulator.setup(
                (subpartitionIndex, buffers) -> accumulatedBuffers.addAll(buffers));

        //        byte[] bytes = new byte[1028];
        //        random.nextBytes(bytes);
        //        ByteBuffer record = ByteBuffer.wrap(bytes);

        int numDataBuffers = 5;
        while (numDataBuffers > 0) {
            // record size may be larger than buffer size so a record may span multiple segments
            int recordSize = random.nextInt(bufferSize * 4 - 1) + 1;
            byte[] bytes = new byte[recordSize];

            // fill record with random value
            random.nextBytes(bytes);
            ByteBuffer record = ByteBuffer.wrap(bytes);

            // select a random subpartition to writeRecord
            int subpartition = random.nextInt(numSubpartitions);
            // select a random data type
            //            boolean isBuffer = random.nextBoolean();
            boolean isBuffer = true;
            Buffer.DataType dataType =
                    isBuffer ? Buffer.DataType.DATA_BUFFER : Buffer.DataType.EVENT_BUFFER;
            sortBufferAccumulator.receive(
                    record, new TieredStorageSubpartitionId(subpartition), dataType, false);
            numDataBuffers--;
        }

        System.out.println(accumulatedBuffers.size());
        int numTotalReceivedSize =
                accumulatedBuffers.stream()
                        .map(Buffer::readableBytes)
                        .mapToInt(Integer::intValue)
                        .sum();
        System.out.println(numTotalReceivedSize);
    }

    private void recycleBuffer(MemorySegment segment) {
        FreeingBufferRecycler.INSTANCE.recycle(segment);
    }

    private BufferWithChannel copyIntoSegment(int bufferSize, DataBuffer dataBuffer) {
        MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(bufferSize);
        return dataBuffer.getNextBuffer(segment);
    }

    private void addBufferRead(
            BufferWithChannel buffer, Queue<Buffer>[] buffersRead, int[] numBytesRead) {
        int channel = buffer.getChannelIndex();
        buffersRead[channel].add(buffer.getBuffer());
        numBytesRead[channel] += buffer.getBuffer().readableBytes();
    }
}
