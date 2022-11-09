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

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.store.common.SingleTierReader;
import org.apache.flink.runtime.io.network.partition.store.common.BufferConsumeView;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** The read view of {@link DfsDataManager}, data can be read from dfs. */
public class DfsFileReader implements SingleTierReader, DfsFileReaderInternalOperations {

    private static final Logger LOG = LoggerFactory.getLogger(DfsFileReader.class);

    private final BufferAvailabilityListener availabilityListener;
    private final Object lock = new Object();

    /** Index of last consumed buffer. */
    @GuardedBy("lock")
    private int lastConsumedBufferIndex = -1;

    @GuardedBy("lock")
    private boolean needNotify = true;

    @Nullable
    @GuardedBy("lock")
    private Buffer.DataType cachedNextDataType = null;

    @Nullable
    @GuardedBy("lock")
    private Throwable failureCause = null;

    @GuardedBy("lock")
    private boolean isReleased = false;

    @Nullable
    @GuardedBy("lock")
    // dfsDataView can be null only before initialization.
    private BufferConsumeView dfsDataView;

    public DfsFileReader(BufferAvailabilityListener availabilityListener) {
        this.availabilityListener = availabilityListener;
    }

    @Nullable
    @Override
    public BufferAndBacklog getNextBuffer() throws IOException {
        try {
            synchronized (lock) {
                checkNotNull(dfsDataView, "disk data view must be not null.");
                Optional<BufferAndBacklog> bufferToConsume =
                        dfsDataView.consumeBuffer(lastConsumedBufferIndex + 1, null);

                if (bufferToConsume.isPresent()) {
                    LOG.debug("### DfsFileReader has successfully consumed one buffer!");
                }

                updateConsumingStatus(bufferToConsume);
                return bufferToConsume.map(this::handleBacklog).orElse(null);
            }
        } catch (Throwable cause) {
            // release subpartition reader outside of lock to avoid deadlock.
            releaseInternal(cause);
            throw new IOException("Failed to get next buffer.", cause);
        }
    }

    @Override
    public void notifyDataAvailable() {
        LOG.debug("%%% Dfs is trying to notify..");
        boolean notifyDownStream = false;
        synchronized (lock) {
            if (isReleased) {
                LOG.debug("%%% Dfs notify failed1");
                return;
            }
            if (needNotify) {
                LOG.debug("%%% Dfs notify failed2");
                notifyDownStream = true;
                needNotify = false;
            }
        }
        // notify outside of lock to avoid deadlock
        if (notifyDownStream) {
            LOG.debug("%%% Dfs notify success1");
            availabilityListener.notifyDataAvailable();
        }
    }

    @Override
    public ResultSubpartitionView.AvailabilityWithBacklog getAvailabilityAndBacklog(
            int numCreditsAvailable) {
        synchronized (lock) {
            boolean availability = numCreditsAvailable > 0;
            if (numCreditsAvailable <= 0
                    && cachedNextDataType != null
                    && cachedNextDataType == Buffer.DataType.EVENT_BUFFER) {
                availability = true;
            }

            int backlog = getSubpartitionBacklog();
            if (backlog == 0) {
                needNotify = true;
            }
            return new ResultSubpartitionView.AvailabilityWithBacklog(availability, backlog);
        }
    }

    @Override
    public void releaseAllResources() throws IOException {
        releaseInternal(null);
    }

    @Override
    public boolean isReleased() {
        synchronized (lock) {
            return isReleased;
        }
    }

    @Override
    public Throwable getFailureCause() {
        synchronized (lock) {
            return failureCause;
        }
    }

    /**
     * Set {@link BufferConsumeView} for this subpartition, this method only called when {@link
     * DfsFileReader} is creating.
     */
    public void setDfsDataView(BufferConsumeView dfsDataView) {
        synchronized (lock) {
            checkState(this.dfsDataView == null, "repeatedly set dfs data view is not allowed.");
            this.dfsDataView = dfsDataView;
        }
    }

    @Override
    public int unsynchronizedGetNumberOfQueuedBuffers() {
        return getSubpartitionBacklog();
    }

    @Override
    public int getNumberOfQueuedBuffers() {
        synchronized (lock) {
            return getSubpartitionBacklog();
        }
    }

    // -------------------------------
    //       Internal Methods
    // -------------------------------

    @SuppressWarnings("FieldAccessNotGuarded")
    private int getSubpartitionBacklog() {
        if (dfsDataView == null) {
            return 0;
        }
        return dfsDataView.getBacklog();
    }

    private BufferAndBacklog handleBacklog(BufferAndBacklog bufferToConsume) {
        BufferAndBacklog bufferAndBacklog = new BufferAndBacklog(
                bufferToConsume.buffer(),
                getSubpartitionBacklog(),
                bufferToConsume.getNextDataType(),
                bufferToConsume.getSequenceNumber(),
                bufferToConsume.isLastBufferInSegment());
        bufferAndBacklog.setFromDfsTier(true);
        return bufferAndBacklog;
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    @GuardedBy("lock")
    private void updateConsumingStatus(Optional<BufferAndBacklog> bufferAndBacklog) {
        assert Thread.holdsLock(lock);
        // if consumed, update and check consume offset
        if (bufferAndBacklog.isPresent()) {
            ++lastConsumedBufferIndex;
            checkState(bufferAndBacklog.get().getSequenceNumber() == lastConsumedBufferIndex);
        }

        // update need-notify
        boolean dataAvailable =
                bufferAndBacklog.map(BufferAndBacklog::isDataAvailable).orElse(false);
        needNotify = !dataAvailable;
        // update cached next data type
        cachedNextDataType = bufferAndBacklog.map(BufferAndBacklog::getNextDataType).orElse(null);
    }

    private void releaseInternal(@Nullable Throwable throwable) {
        boolean releaseDfsView;
        synchronized (lock) {
            if (isReleased) {
                return;
            }
            isReleased = true;
            failureCause = throwable;
            releaseDfsView = dfsDataView != null;
        }

        if (throwable != null) {
            LOG.debug("Release the dfs subpartition consumer. ", throwable);
        }

        if (releaseDfsView) {
            //noinspection FieldAccessNotGuarded
            dfsDataView.releaseDataView();
        }
    }
}
