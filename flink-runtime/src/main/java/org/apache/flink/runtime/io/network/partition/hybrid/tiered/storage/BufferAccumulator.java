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

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.OutputMetrics;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.BiConsumer;

public interface BufferAccumulator {

    /**
     * Setup the accumulator.
     *
     * @param numSubpartitions number of subpartitions
     * @param bufferFlusher accepts the accumulated buffers. The index of the outer list corresponds
     *     to the subpartition ids, while each inner list contains accumulated buffers in order for
     *     that subpartition.
     */
    void setup(int numSubpartitions, BiConsumer<Integer, List<Buffer>> bufferFlusher);

    /**
     * Receives the records from {@link TieredStorageProducerClient}, these records will be
     * accumulated and transformed into finished buffers.
     */
    void receive(ByteBuffer record, int consumerId, Buffer.DataType dataType) throws IOException;

    void setMetricGroup(OutputMetrics metrics);

    void close();

    void release();
}
