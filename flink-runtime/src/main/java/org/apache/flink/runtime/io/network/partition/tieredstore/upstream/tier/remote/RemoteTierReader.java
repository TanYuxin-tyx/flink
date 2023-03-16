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

package org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.remote;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.BufferContext;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierReaderImpl;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierReaderViewId;

import java.util.Optional;
import java.util.concurrent.locks.Lock;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.DATA_BUFFER;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** The {@link RemoteTierReader} is used to consume data from Remote Tier. */
public class RemoteTierReader extends TierReaderImpl {

    private final TierReaderViewId tierReaderViewId;

    private final int subpartitionId;

    private final RemoteCacheManagerOperation remoteCacheManagerOperation;

    public RemoteTierReader(
            Lock consumerLock,
            int subpartitionId,
            TierReaderViewId tierReaderViewId,
            RemoteCacheManagerOperation memoryDataWriterOperation) {
        super(consumerLock);
        this.subpartitionId = subpartitionId;
        this.tierReaderViewId = tierReaderViewId;
        this.remoteCacheManagerOperation = memoryDataWriterOperation;
    }

    @Override
    public void release() {
        remoteCacheManagerOperation.onConsumerReleased(subpartitionId, tierReaderViewId);
    }

    @Override
    public Optional<ResultSubpartition.BufferAndBacklog> consumeBuffer(int toConsumeIndex) {
        Optional<Tuple2<BufferContext, Buffer.DataType>> bufferAndNextDataType =
                callWithLock(
                        () -> {
                            if (unConsumedBuffers.isEmpty()) {
                                return Optional.empty();
                            }
                            BufferContext bufferContext =
                                    checkNotNull(unConsumedBuffers.pollFirst());
                            return Optional.of(Tuple2.of(bufferContext, null));
                        });
        return bufferAndNextDataType.map(
                tuple ->
                        new ResultSubpartition.BufferAndBacklog(
                                null, -1, DATA_BUFFER, toConsumeIndex, true));
    }
}
