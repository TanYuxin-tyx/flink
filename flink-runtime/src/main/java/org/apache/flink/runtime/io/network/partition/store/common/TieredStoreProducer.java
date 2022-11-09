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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.ChannelStateHolder;
import org.apache.flink.runtime.io.network.partition.CheckpointedResultPartition;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/** The producer interface of Tiered Store, data can be written to different store tiers. */
public interface TieredStoreProducer extends ChannelStateHolder, CheckpointedResultPartition {

    void emit(
            ByteBuffer record,
            int targetSubpartition,
            Buffer.DataType dataType,
            boolean isBroadcast,
            boolean isEndOfPartition)
            throws IOException;

    void close();

    void release();

    @VisibleForTesting
    void setNumBytesInASegment(int numBytesInASegment);

    void alignedBarrierTimeout(long checkpointId) throws IOException;

    void abortCheckpoint(long checkpointId, CheckpointException cause);

    void flushAll();

    void flush(int subpartitionIndex);

    int getNumberOfQueuedBuffers();

    long getSizeOfQueuedBuffersUnsafe();

    int getNumberOfQueuedBuffers(int targetSubpartition);

    void onConsumedSubpartition(int subpartitionIndex);

    CompletableFuture<Void> getAllDataProcessedFuture();

    void onSubpartitionAllDataProcessed(int subpartition);
}
