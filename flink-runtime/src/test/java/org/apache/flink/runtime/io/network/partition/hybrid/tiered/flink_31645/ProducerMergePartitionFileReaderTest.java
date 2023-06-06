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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.flink_31645;

import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.NoOpBufferAvailablityListener;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.NettyPayload;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file.PartitionFileReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file.ProducerMergePartitionFileReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.local.disk.DiskCacheBufferSpiller;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.local.disk.RegionBufferIndexTracker;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.local.disk.RegionBufferIndexTrackerImpl;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;

/** Tests for {@link ProducerMergePartitionFileReader}. */
class ProducerMergePartitionFileReaderTest {

    private static final int NUM_SUBPARTITIONS = 1;

    private static final int SUBPARTITION_ID = 0;

    private static final int BUFFER_NUM_PER_SUBPARTITION = 2;

    private static final int BUFFER_SIZE = 1024;

    private static final int BUFFER_POOL_SIZE = NUM_SUBPARTITIONS * BUFFER_NUM_PER_SUBPARTITION;

    private final byte[] dataBytes = new byte[BUFFER_SIZE];

    private BatchShuffleReadBufferPool bufferPool;

    private FileChannel dataFileChannel;

    private Path dataFilePath;

    private PartitionFileReader producerMergePartitionFileReader;

    private RegionBufferIndexTracker regionBufferIndexTracker;

    private final BufferAvailabilityListener defaultAvailabilityListener =
            new NoOpBufferAvailablityListener();

    //private final NettyServiceWriterId defaultNettyServiceWriterId = NettyServiceWriterId.DEFAULT;

    @BeforeEach
    void before(@TempDir Path tempDir)
            throws IOException, ExecutionException, InterruptedException {
        Random random = new Random();
        random.nextBytes(dataBytes);
        bufferPool = new BatchShuffleReadBufferPool(BUFFER_POOL_SIZE * BUFFER_SIZE, BUFFER_SIZE);
        bufferPool.initialize();
        generateShuffleData(tempDir);
        dataFileChannel = openFileChannel(dataFilePath);
    }

    @AfterEach
    void after() throws Exception {
        bufferPool.destroy();
        if (dataFileChannel != null) {
            dataFileChannel.close();
        }
    }

    //@Test
    //void testRegisterNettyService() throws Exception {
    //    producerMergePartitionFileReader = createProducerMergePartitionFileReader();
    //    CreditBasedBufferQueueView creditBasedBufferQueueView =
    //            producerMergePartitionFileReader.registerNettyService(
    //                    SUBPARTITION_ID,
    //                    defaultCreditBasedShuffleViewId, defaultAvailabilityListener);
    //    int backlog;
    //    do {
    //        backlog = creditBasedBufferQueueView.getBacklog();
    //        TimeUnit.MILLISECONDS.sleep(50);
    //    } while (backlog != BUFFER_NUM_PER_SUBPARTITION);
    //    assertThat(creditBasedBufferQueueView.getBacklog())
    //            .isEqualTo(BUFFER_NUM_PER_SUBPARTITION);
    //}
    //
    //@Test
    //void testBuffersAreCorrectlyReleased() throws Throwable {
    //    producerMergePartitionFileReader = createProducerMergePartitionFileReader();
    //    CreditBasedBufferQueueView creditBasedBufferQueueView =
    //            producerMergePartitionFileReader.registerNettyService(
    //                    SUBPARTITION_ID,
    //                    defaultCreditBasedShuffleViewId, defaultAvailabilityListener);
    //    int numberOfQueuedBuffers;
    //    do {
    //        numberOfQueuedBuffers = creditBasedBufferQueueView.getBacklog();
    //        TimeUnit.MILLISECONDS.sleep(50);
    //    } while (numberOfQueuedBuffers != BUFFER_NUM_PER_SUBPARTITION);
    //    assertThat(numberOfQueuedBuffers).isEqualTo(BUFFER_NUM_PER_SUBPARTITION);
    //    for (int bufferIndex = 0; bufferIndex < numberOfQueuedBuffers; ++bufferIndex) {
    //        Optional<Buffer> nextBuffer = creditBasedBufferQueueView.getNextBuffer();
    //        assertThat(nextBuffer).isPresent();
    //        nextBuffer.get().recycleBuffer();
    //    }
    //    assertThat(bufferPool.getAvailableBuffers()).isEqualTo(BUFFER_POOL_SIZE);
    //}

    @Test
    void testRegisterSubpartitionReaderAfterReleased() {
        //producerMergePartitionFileReader = createProducerMergePartitionFileReader();
        //producerMergePartitionFileReader.release();
        //assertThatThrownBy(
        //                () -> {
        //                    producerMergePartitionFileReader.registerNettyService(
        //
        //                            SUBPARTITION_ID,
        //                            defaultCreditBasedShuffleViewId,
        //                            defaultAvailabilityListener);
        //                })
        //        .isInstanceOf(IllegalStateException.class)
        //        .hasMessageContaining("already released.");
    }

    private FileChannel openFileChannel(Path path) throws IOException {
        return FileChannel.open(path, StandardOpenOption.READ);
    }

    //private PartitionFileReader createProducerMergePartitionFileReader() {
    //    return new ProducerMergePartitionFileReader(
    //            new ResultPartitionID(),
    //            bufferPool,
    //            Executors.newScheduledThreadPool(1),
    //            regionBufferIndexTracker,
    //            dataFilePath,
    //            bufferPool.getNumBuffersPerRequest(),
    //            Duration.ofDays(1),
    //            5,
    //            new TieredStorageNettyServiceImpl());
    //}

    private void generateShuffleData(Path tempDir)
            throws IOException, ExecutionException, InterruptedException {
        dataFilePath = tempDir.resolve(".data");
        regionBufferIndexTracker = new RegionBufferIndexTrackerImpl(NUM_SUBPARTITIONS);
        DiskCacheBufferSpiller spiller =
                new DiskCacheBufferSpiller(dataFilePath, regionBufferIndexTracker);
        spiller.spillAsync(generateBufferContexts()).get();
    }

    private List<NettyPayload> generateBufferContexts() {
        List<NettyPayload> nettyPayloads = new ArrayList<>();
        for (int subpartitionId = 0; subpartitionId < NUM_SUBPARTITIONS; ++subpartitionId) {
            for (int bufferIndex = 0; bufferIndex <= BUFFER_NUM_PER_SUBPARTITION; ++bufferIndex) {
                nettyPayloads.add(
                        new NettyPayload(
                                new NetworkBuffer(
                                        MemorySegmentFactory.wrap(new byte[BUFFER_SIZE]),
                                        FreeingBufferRecycler.INSTANCE),
                                bufferIndex,
                                subpartitionId));
            }
        }
        return nettyPayloads;
    }
}
