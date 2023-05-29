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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.Buffer.DataType;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.BufferContext;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.Optional;
import java.util.Queue;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** The implementation of {@link CreditBasedBufferQueueView}. */
public class CreditBasedBufferQueueViewImpl implements CreditBasedBufferQueueView {

    private final BufferAvailabilityListener availabilityListener;

    private int consumedBufferIndex = -1;

    @GuardedBy("viewLock")
    private boolean isReleased = false;

    @GuardedBy("viewLock")
    private final Queue<BufferContext> bufferQueue;

    private final Runnable releaseNotifier;

    public CreditBasedBufferQueueViewImpl(
            Queue<BufferContext> bufferQueue,
            BufferAvailabilityListener availabilityListener,
            Runnable releaseNotifier) {
        this.bufferQueue = bufferQueue;
        this.availabilityListener = availabilityListener;
        this.releaseNotifier = releaseNotifier;
    }

    @Override
    public Optional<Buffer> getNextBuffer() throws IOException {
        BufferContext buffer = bufferQueue.poll();
        if (buffer == null) {
            return Optional.empty();
        } else {
            Throwable readError = buffer.getError();
            if (readError != null) {
                release();
                throw new IOException(readError);
            } else {
                checkState(buffer.getBufferIndex() == ++consumedBufferIndex);
                return Optional.of(checkNotNull(buffer.getBuffer()));
            }
        }
    }

    @Override
    public int getBacklog() {
        return bufferQueue.size();
    }

    @Override
    public DataType getNextBufferDataType() {
        BufferContext nextBuffer = bufferQueue.peek();
        if (nextBuffer == null || nextBuffer.getBuffer() == null) {
            return DataType.NONE;
        } else {
            return nextBuffer.getBuffer().getDataType();
        }
    }

    @Override
    public void notifyDataAvailable() {
        if (isReleased) {
            return;
        }
        availabilityListener.notifyDataAvailable();
    }

    @Override
    public void release() {
        if (isReleased) {
            return;
        }
        isReleased = true;
        BufferContext bufferContext;
        while ((bufferContext = bufferQueue.poll()) != null) {
            if (bufferContext.getBuffer() != null) {
                checkNotNull(bufferContext.getBuffer()).recycleBuffer();
            }
        }
        releaseNotifier.run();
    }
}
