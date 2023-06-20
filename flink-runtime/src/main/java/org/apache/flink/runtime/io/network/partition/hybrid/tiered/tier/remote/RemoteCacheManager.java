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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.remote;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.PartitionFileWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FatalExitExceptionHandler;

import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** This class is responsible for managing cached buffers data before flush to remote storage. */
public class RemoteCacheManager {

    private final int numSubpartitions;

    private final SubpartitionRemoteCacheManager[] subpartitionCacheDataManagers;

    private final ExecutorService ioExecutor =
            Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder()
                            .setNameFormat("tiered store remote spiller")
                            // It is more appropriate to use task fail over than exit JVM here,
                            // but the task thread will bring some extra overhead to check the
                            // exception information set by other thread. As the spiller thread will
                            // not encounter exceptions in most cases, we temporarily choose the
                            // form of fatal error to deal except thrown by spiller thread.
                            .setUncaughtExceptionHandler(FatalExitExceptionHandler.INSTANCE)
                            .build());

    public RemoteCacheManager(
            int numSubpartitions,
            TieredStorageMemoryManager storageMemoryManager,
            PartitionFileWriter partitionFileWriter) {
        this.numSubpartitions = numSubpartitions;
        this.subpartitionCacheDataManagers = new SubpartitionRemoteCacheManager[numSubpartitions];
        for (int subpartitionId = 0; subpartitionId < numSubpartitions; ++subpartitionId) {
            subpartitionCacheDataManagers[subpartitionId] =
                    new SubpartitionRemoteCacheManager(
                            subpartitionId, storageMemoryManager, partitionFileWriter);
        }
    }

    // ------------------------------------
    //          For DfsDataManager
    // ------------------------------------

    public void appendBuffer(Buffer finishedBuffer, int targetChannel) {
        getSubpartitionCacheDataManager(targetChannel).addFinishedBuffer(finishedBuffer);
    }

    public void startSegment(int targetSubpartition, int segmentIndex) {
        getSubpartitionCacheDataManager(targetSubpartition).startSegment(segmentIndex);
    }

    public void finishSegment(int targetSubpartition, int segmentIndex) {
        getSubpartitionCacheDataManager(targetSubpartition).finishSegment(segmentIndex);
    }

    /** Close this {@link RemoteCacheManager}, it means no data can append to memory. */
    public void close() {
        Arrays.stream(subpartitionCacheDataManagers).forEach(SubpartitionRemoteCacheManager::close);
        try {
            ioExecutor.shutdown();
            if (!ioExecutor.awaitTermination(5L, TimeUnit.MINUTES)) {
                throw new TimeoutException("Shutdown spilling thread timeout.");
            }
        } catch (Exception e) {
            ExceptionUtils.rethrow(e);
        }
    }

    /**
     * Release this {@link RemoteCacheManager}, it means all memory taken by this class will
     * recycle.
     */
    public void release() {
        for (int i = 0; i < numSubpartitions; i++) {
            getSubpartitionCacheDataManager(i).release();
        }
    }

    // ------------------------------------
    //           Internal Method
    // ------------------------------------

    private SubpartitionRemoteCacheManager getSubpartitionCacheDataManager(int targetChannel) {
        return subpartitionCacheDataManagers[targetChannel];
    }
}
