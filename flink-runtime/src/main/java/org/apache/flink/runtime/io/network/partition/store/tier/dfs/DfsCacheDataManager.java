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

package org.apache.flink.runtime.io.network.partition.store.tier.dfs;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.store.common.BufferPoolHelper;
import org.apache.flink.runtime.io.network.partition.store.common.ConsumerId;
import org.apache.flink.runtime.io.network.partition.store.common.BufferConsumeView;
import org.apache.flink.runtime.io.network.partition.store.tier.local.file.OutputMetrics;
import org.apache.flink.runtime.io.network.partition.store.tier.local.file.SubpartitionConsumerCacheDataManager;
import org.apache.flink.runtime.io.network.partition.store.tier.local.file.SubpartitionConsumerInternalOperations;
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
public class DfsCacheDataManager implements DfsCacheDataManagerOperation {
    private static final Logger LOG = LoggerFactory.getLogger(DfsCacheDataManager.class);

    private final int numSubpartitions;

    private final SubpartitionDfsCacheDataManager[] subpartitionCacheDataManagers;

    private final Lock lock;

    /**
     * Each element of the list is all views of the subpartition corresponding to its index, which
     * are stored in the form of a map that maps consumer id to its subpartition view.
     */
    private final List<Map<ConsumerId, DfsFileReaderInternalOperations>>
            subpartitionViewOperationsMap;

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

    public DfsCacheDataManager(
            JobID jobID,
            ResultPartitionID resultPartitionID,
            int numSubpartitions,
            int bufferSize,
            boolean isBroadcastOnly,
            String baseDfsPath,
            BufferPoolHelper bufferPoolHelper,
            BufferCompressor bufferCompressor)
            throws IOException {
        this.numSubpartitions = numSubpartitions;
        this.subpartitionCacheDataManagers = new SubpartitionDfsCacheDataManager[numSubpartitions];

        ReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);
        this.lock = readWriteLock.writeLock();

        this.subpartitionViewOperationsMap = new ArrayList<>(numSubpartitions);
        for (int subpartitionId = 0; subpartitionId < numSubpartitions; ++subpartitionId) {
            subpartitionCacheDataManagers[subpartitionId] =
                    new SubpartitionDfsCacheDataManager(
                            jobID,
                            resultPartitionID,
                            subpartitionId,
                            bufferSize,
                            isBroadcastOnly,
                            bufferPoolHelper,
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

    /**
     * Append record to {@link
     * org.apache.flink.runtime.io.network.partition.store.tier.local.file.CacheDataManager}, It
     * will be managed by {@link SubpartitionConsumerCacheDataManager} witch it belongs to.
     *
     * @param record to be managed by this class.
     * @param targetChannel target subpartition of this record.
     * @param dataType the type of this record. In other words, is it data or event.
     * @param isLastRecordInSegment whether this record is the last record in a segment.
     */
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

    public void startSegment(int targetSubpartition, long segmentIndex) throws IOException {
        getSubpartitionCacheDataManager(targetSubpartition).startSegment(segmentIndex);
    }

    public void finishSegment(int targetSubpartition, long segmentIndex) {
        getSubpartitionCacheDataManager(targetSubpartition).finishSegment(segmentIndex);
    }

    /**
     * Register {@link SubpartitionConsumerInternalOperations} to {@link
     * #subpartitionViewOperationsMap}. It is used to obtain the consumption progress of the
     * subpartition.
     */
    public BufferConsumeView registerNewConsumer(
            int subpartitionId,
            ConsumerId consumerId,
            DfsFileReaderInternalOperations viewOperations) {
        LOG.debug("### registered, subpartition {}, consumerId {},", subpartitionId, consumerId);
        DfsFileReaderInternalOperations oldView =
                subpartitionViewOperationsMap.get(subpartitionId).put(consumerId, viewOperations);
        Preconditions.checkState(
                oldView == null, "Each subpartition view should have unique consumerId.");
        return getSubpartitionCacheDataManager(subpartitionId).registerNewConsumer(consumerId);
    }

    /**
     * Close this {@link
     * org.apache.flink.runtime.io.network.partition.store.tier.local.file.CacheDataManager}, it
     * means no data can append to memory.
     */
    public void close() {
        Arrays.stream(subpartitionCacheDataManagers)
                .forEach(SubpartitionDfsCacheDataManager::close);
        ioExecutor.shutdown();
    }

    /**
     * Release this {@link
     * org.apache.flink.runtime.io.network.partition.store.tier.local.file.CacheDataManager}, it
     * means all memory taken by this class will recycle.
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
    public void onDataAvailable(int subpartitionId, Collection<ConsumerId> consumerIds) {
        Map<ConsumerId, DfsFileReaderInternalOperations> consumerViewMap =
                subpartitionViewOperationsMap.get(subpartitionId);
        consumerIds.forEach(
                consumerId -> {
                    DfsFileReaderInternalOperations consumerView = consumerViewMap.get(consumerId);
                    if (consumerView != null) {
                        consumerView.notifyDataAvailable();
                    }
                });
    }

    @Override
    public void onConsumerReleased(int subpartitionId, ConsumerId consumerId) {
        LOG.debug("### Release subpartitionId {}, consumerId {}", subpartitionId, consumerId);
        subpartitionViewOperationsMap.get(subpartitionId).remove(consumerId);
        getSubpartitionCacheDataManager(subpartitionId).releaseConsumer(consumerId);
    }

    // ------------------------------------
    //           Internal Method
    // ------------------------------------

    private SubpartitionDfsCacheDataManager getSubpartitionCacheDataManager(int targetChannel) {
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
