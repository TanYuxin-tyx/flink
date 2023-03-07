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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreMemoryManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.CacheFlushManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierReader;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierReaderViewId;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.DiskCacheManager;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.OutputMetrics;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.tier.local.disk.SubpartitionDiskReaderViewOperations;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FatalExitExceptionHandler;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SupplierWithException;

import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** This class is responsible for managing cached buffers data before flush to DFS files. */
public class RemoteCacheManager implements RemoteCacheManagerOperation {
    private static final Logger LOG = LoggerFactory.getLogger(RemoteCacheManager.class);

    private final int numSubpartitions;

    private final SubpartitionRemoteCacheManager[] subpartitionCacheDataManagers;

    private final Lock lock;

    /**
     * Each element of the list is all views of the subpartition corresponding to its index, which
     * are stored in the form of a map that maps consumer id to its subpartition view.
     */
    private final List<Map<TierReaderViewId, RemoteReaderOperations>> subpartitionViewOperationsMap;

    private final ExecutorService ioExecutor =
            Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder()
                            .setNameFormat("tiered store dfs spiller")
                            // It is more appropriate to use task fail over than exit JVM here,
                            // but the task thread will bring some extra overhead to check the
                            // exception information set by other thread. As the spiller thread will
                            // not encounter exceptions in most cases, we temporarily choose the
                            // form of fatal error to deal except thrown by spiller thread.
                            .setUncaughtExceptionHandler(FatalExitExceptionHandler.INSTANCE)
                            .build());

    public RemoteCacheManager(
            JobID jobID,
            ResultPartitionID resultPartitionID,
            int numSubpartitions,
            int bufferSize,
            boolean isBroadcastOnly,
            String baseDfsPath,
            TieredStoreMemoryManager tieredStoreMemoryManager,
            CacheFlushManager cacheFlushManager,
            BufferCompressor bufferCompressor)
            throws IOException {
        this.numSubpartitions = numSubpartitions;
        this.subpartitionCacheDataManagers = new SubpartitionRemoteCacheManager[numSubpartitions];

        ReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);
        this.lock = readWriteLock.writeLock();

        this.subpartitionViewOperationsMap = new ArrayList<>(numSubpartitions);
        for (int subpartitionId = 0; subpartitionId < numSubpartitions; ++subpartitionId) {
            subpartitionCacheDataManagers[subpartitionId] =
                    new SubpartitionRemoteCacheManager(
                            jobID,
                            resultPartitionID,
                            subpartitionId,
                            bufferSize,
                            isBroadcastOnly,
                            tieredStoreMemoryManager,
                            cacheFlushManager,
                            baseDfsPath,
                            readWriteLock.readLock(),
                            bufferCompressor,
                            this,
                            ioExecutor);
            subpartitionViewOperationsMap.add(new ConcurrentHashMap<>());
        }
    }

    // ------------------------------------
    //          For DfsDataManager
    // ------------------------------------

    public void append(
            ByteBuffer record,
            int targetChannel,
            Buffer.DataType dataType,
            boolean isLastRecordInSegment)
            throws IOException {
        try {
            getSubpartitionCacheDataManager(targetChannel)
                    .append(record, dataType, isLastRecordInSegment);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    public void startSegment(int targetSubpartition, int segmentIndex) throws IOException {
        getSubpartitionCacheDataManager(targetSubpartition).startSegment(segmentIndex);
    }

    public void finishSegment(int targetSubpartition, int segmentIndex) {
        getSubpartitionCacheDataManager(targetSubpartition).finishSegment(segmentIndex);
    }

    /**
     * Register {@link SubpartitionDiskReaderViewOperations} to {@link
     * #subpartitionViewOperationsMap}. It is used to obtain the consumption progress of the
     * subpartition.
     */
    public TierReader registerNewConsumer(
            int subpartitionId,
            TierReaderViewId tierReaderViewId,
            RemoteReaderOperations viewOperations) {
        LOG.debug(
                "### registered, subpartition {}, consumerId {},",
                subpartitionId,
                tierReaderViewId);
        RemoteReaderOperations oldView =
                subpartitionViewOperationsMap
                        .get(subpartitionId)
                        .put(tierReaderViewId, viewOperations);
        Preconditions.checkState(
                oldView == null, "Each subpartition view should have unique consumerId.");
        return getSubpartitionCacheDataManager(subpartitionId)
                .registerNewConsumer(tierReaderViewId);
    }

    /** Close this {@link DiskCacheManager}, it means no data can append to memory. */
    public void close() {
        Arrays.stream(subpartitionCacheDataManagers).forEach(SubpartitionRemoteCacheManager::close);
        ioExecutor.shutdown();
    }

    /**
     * Release this {@link DiskCacheManager}, it means all memory taken by this class will recycle.
     */
    public void release() {
        for (int i = 0; i < numSubpartitions; i++) {
            getSubpartitionCacheDataManager(i).release();
        }
        // TODO delete the shuffle files
        try {
            ioExecutor.shutdown();
            if (!ioExecutor.awaitTermination(5L, TimeUnit.MINUTES)) {
                throw new TimeoutException("Shutdown spilling thread timeout.");
            }
        } catch (Exception e) {
            ExceptionUtils.rethrow(e);
        }
    }

    private void deleteAllTheShuffleFiles() {}

    public void setOutputMetrics(OutputMetrics metrics) {
        // HsOutputMetrics is not thread-safe. It can be shared by all the subpartitions because it
        // is expected always updated from the producer task's mailbox thread.
        for (int i = 0; i < numSubpartitions; i++) {
            getSubpartitionCacheDataManager(i).setOutputMetrics(metrics);
        }
    }

    // ------------------------------------
    //      Callback for subpartition
    // ------------------------------------

    @Override
    public void onDataAvailable(
            int subpartitionId, Collection<TierReaderViewId> tierReaderViewIds) {
        Map<TierReaderViewId, RemoteReaderOperations> consumerViewMap =
                subpartitionViewOperationsMap.get(subpartitionId);
        tierReaderViewIds.forEach(
                consumerId -> {
                    RemoteReaderOperations consumerView = consumerViewMap.get(consumerId);
                    if (consumerView != null) {
                        consumerView.notifyDataAvailable();
                    }
                });
    }

    @Override
    public void onConsumerReleased(int subpartitionId, TierReaderViewId tierReaderViewId) {
        LOG.debug("### Release subpartitionId {}, consumerId {}", subpartitionId, tierReaderViewId);
        subpartitionViewOperationsMap.get(subpartitionId).remove(tierReaderViewId);
        getSubpartitionCacheDataManager(subpartitionId).releaseConsumer(tierReaderViewId);
    }

    // ------------------------------------
    //           Internal Method
    // ------------------------------------

    private SubpartitionRemoteCacheManager getSubpartitionCacheDataManager(int targetChannel) {
        return subpartitionCacheDataManagers[targetChannel];
    }

    private <T, R extends Exception> T callWithLock(SupplierWithException<T, R> callable) throws R {
        try {
            lock.lock();
            return callable.get();
        } finally {
            lock.unlock();
        }
    }

    @VisibleForTesting
    public Path getBaseSubpartitionPath(int subpartitionId) {
        return subpartitionCacheDataManagers[subpartitionId].getBaseSubpartitionPath();
    }
}