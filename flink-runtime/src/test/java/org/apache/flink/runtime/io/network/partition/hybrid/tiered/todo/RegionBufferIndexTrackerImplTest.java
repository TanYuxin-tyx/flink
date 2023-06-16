///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.flink.runtime.io.network.partition.hybrid.tiered.todo;
//
//import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file.PartitionFileIndex;
//import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file.PartitionFileIndex.ReadableRegion;
//import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file.PartitionFileIndex.SpilledBuffer;
//import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file.PartitionFileIndexImpl;
//import org.apache.flink.util.TestLoggerExtension;
//
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//import org.junit.jupiter.api.extension.ExtendWith;
//
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.Collections;
//import java.util.List;
//import java.util.Optional;
//
//import static org.assertj.core.api.Assertions.assertThat;
//
///** Tests for {@link PartitionFileIndexImpl}. */
//@ExtendWith(TestLoggerExtension.class)
//class RegionBufferIndexTrackerImplTest {
//    private static final int NUM_SUBPARTITIONS = 2;
//
//    private PartitionFileIndex partitionFileIndex;
//
//    @BeforeEach
//    void before() {
//        partitionFileIndex = new PartitionFileIndexImpl(NUM_SUBPARTITIONS);
//    }
//
//    /**
//     * If the buffer index with the corresponding subpartition does not exist in the data index, or
//     * no buffer has ever been added to the subpartition. The return value should be {@link
//     * Optional#empty()}.
//     */
//    @Test
//    void testGetReadableRegionBufferNotExist() {
//        partitionFileIndex.addRegionIndex(createSpilledBuffers(0, Arrays.asList(0, 2)));
//        // subpartition 0 does not have buffer with index 1
//        // assertThat(regionBufferIndexTracker.getNextRegion(0, 1, -1,
//        // NettyServiceViewId.DEFAULT))
//        //        .isNotPresent();
//        //// subpartition 1 has no buffer
//        // assertThat(regionBufferIndexTracker.getNextRegion(1, 0, -1,
//        // NettyServiceViewId.DEFAULT))
//        //        .isNotPresent();
//    }
//
//    /** If target buffer is not readable, {@link Optional#empty()} should be eventually returned. */
//    @Test
//    void testGetReadableRegionNotReadable() {
//        partitionFileIndex.addRegionIndex(createSpilledBuffers(0, Collections.singletonList(0)));
//        // 0-0 is not readable as consuming offset is bigger than 0.
//        // assertThat(regionBufferIndexTracker.getNextRegion(0, 0, 1,
//        // NettyServiceViewId.DEFAULT))
//        //        .isNotPresent();
//    }
//
//    /**
//     * If target buffer is already readable, a not null {@link ReadableRegion} starts with the given
//     * buffer index should be returned.
//     */
//    @Test
//    void testGetReadableRegion() {
//        final int subpartitionId = 0;
//
//        partitionFileIndex.addRegionIndex(
//                createSpilledBuffers(subpartitionId, Arrays.asList(0, 1, 3, 4, 5)));
//        // assertThat(
//        //                regionBufferIndexTracker.getNextRegion(
//        //                        subpartitionId, 1, 0, TierReaderViewId.DEFAULT))
//        //        .hasValueSatisfying(
//        //                readableRegion -> {
//        //                    assertRegionStartWithTargetBufferIndex(readableRegion, 1);
//        //                    // Readable region will not include discontinuous buffer.
//        //                    assertThat(readableRegion.numReadable).isEqualTo(1);
//        //                });
//        // assertThat(
//        //                regionBufferIndexTracker.getNextRegion(
//        //                        subpartitionId, 3, 0, TierReaderViewId.DEFAULT))
//        //        .hasValueSatisfying(
//        //                readableRegion -> {
//        //                    assertRegionStartWithTargetBufferIndex(readableRegion, 3);
//        //                    assertThat(readableRegion.numReadable)
//        //                            .isGreaterThanOrEqualTo(1)
//        //                            .isLessThanOrEqualTo(2);
//        //                });
//        // assertThat(
//        //                regionBufferIndexTracker.getNextRegion(
//        //                        subpartitionId, 4, 0, TierReaderViewId.DEFAULT))
//        //        .hasValueSatisfying(
//        //                readableRegion -> {
//        //                    assertRegionStartWithTargetBufferIndex(readableRegion, 4);
//        //                    assertThat(readableRegion.numReadable).isEqualTo(1);
//        //                });
//    }
//
//    /**
//     * Verify that the offset of the first buffer of the region is the offset of the target buffer.
//     */
//    private static void assertRegionStartWithTargetBufferIndex(
//            ReadableRegion readableRegion, int targetBufferIndex) {
//        assertThat(targetBufferIndex).isEqualTo(readableRegion.offset + readableRegion.numSkip);
//    }
//
//    /** Note that: To facilitate testing, offset are set to be equal to buffer index. */
//    private static List<SpilledBuffer> createSpilledBuffers(
//            int subpartitionId, List<Integer> bufferIndexes) {
//        List<SpilledBuffer> spilledBuffers = new ArrayList<>();
//        for (int bufferIndex : bufferIndexes) {
//            spilledBuffers.add(new SpilledBuffer(subpartitionId, bufferIndex, bufferIndex));
//        }
//        return spilledBuffers;
//    }
//}
