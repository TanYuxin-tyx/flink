package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file;

import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.BufferContext;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.service.NettyBufferQueue;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.service.NettyBufferQueueImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.service.NettyServiceView;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.service.NettyServiceViewId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.local.disk.RegionBufferIndexTracker;
import org.apache.flink.util.FatalExitExceptionHandler;
import org.apache.flink.util.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** THe implementation of {@link PartitionFileReader} for merged subpartition files. */
public class ProducerMergePartitionFileReader
        implements Runnable, BufferRecycler, PartitionFileReader {

    private static final Logger LOG =
            LoggerFactory.getLogger(ProducerMergePartitionFileReader.class);

    private final ScheduledExecutorService ioExecutor;

    private final Duration bufferRequestTimeout;

    private final Object lock = new Object();

    private final BatchShuffleReadBufferPool bufferPool;

    private final Path dataFilePath;

    private final RegionBufferIndexTracker dataIndex;

    private final ByteBuffer headerBuf = BufferReaderWriterUtil.allocatedHeaderBuffer();

    @GuardedBy("lock")
    private final Set<ProducerMergePartitionSubpartitionReader> allSubpartitionReaders =
            new HashSet<>();

    private final int maxBufferReadAhead;

    private final int maxRequestedBuffers;

    @GuardedBy("lock")
    private FileChannel dataFileChannel;

    @GuardedBy("lock")
    private boolean isRunning;

    @GuardedBy("lock")
    private volatile int numRequestedBuffers;

    @GuardedBy("lock")
    private volatile boolean isReleased;

    public ProducerMergePartitionFileReader(
            BatchShuffleReadBufferPool bufferPool,
            ScheduledExecutorService ioExecutor,
            RegionBufferIndexTracker dataIndex,
            Path dataFilePath,
            int maxRequestedBuffers,
            Duration bufferRequestTimeout,
            int maxBufferReadAhead) {
        this.dataIndex = checkNotNull(dataIndex);
        this.dataFilePath = checkNotNull(dataFilePath);
        this.bufferPool = checkNotNull(bufferPool);
        this.ioExecutor = checkNotNull(ioExecutor);
        this.maxRequestedBuffers = maxRequestedBuffers;
        this.bufferRequestTimeout = checkNotNull(bufferRequestTimeout);
        this.maxBufferReadAhead = maxBufferReadAhead;
    }

    /** Setup read buffer pool. */
    public void setup() {
        bufferPool.initialize();
    }

    @Override
    public synchronized void run() {
        int numBuffersRead = read();
        endCurrentRoundOfReading(numBuffersRead);
    }

    @Override
    public int read() {
        List<ProducerMergePartitionSubpartitionReader> availableReaders = sortAvailableReaders();
        if (availableReaders.isEmpty()) {
            return 0;
        }
        Queue<MemorySegment> buffers;
        try {
            buffers = allocateBuffers();
        } catch (Exception exception) {
            // fail all pending subpartition readers immediately if any exception occurs
            failSubpartitionReaders(availableReaders, exception);
            LOG.error("Failed to request buffers for data reading.", exception);
            return 0;
        }

        int numBuffersAllocated = buffers.size();
        if (numBuffersAllocated <= 0) {
            return 0;
        }

        readData(availableReaders, buffers);
        int numBuffersRead = numBuffersAllocated - buffers.size();

        releaseBuffers(buffers);

        return numBuffersRead;
    }

    @Override
    public NettyBufferQueue createNettyBufferQueue(
            int subpartitionId,
            NettyServiceViewId nettyServiceViewId,
            NettyServiceView tierConsumerView)
            throws IOException {
        synchronized (lock) {
            checkState(!isReleased, "ProducerMergePartitionFileReader is already released.");
            lazyInitialize();
            LinkedBlockingDeque<BufferContext> bufferQueue = new LinkedBlockingDeque<>();
            ProducerMergePartitionSubpartitionReader subpartitionReader =
                    new ProducerMergePartitionSubpartitionReaderImpl(
                            subpartitionId,
                            maxBufferReadAhead,
                            bufferQueue,
                            headerBuf,
                            nettyServiceViewId,
                            dataFileChannel,
                            tierConsumerView,
                            dataIndex);
            allSubpartitionReaders.add(subpartitionReader);
            mayTriggerReading();
            return new NettyBufferQueueImpl(
                    bufferQueue,
                    () -> {
                        synchronized (lock) {
                            removeSubpartitionReaders(Collections.singleton(subpartitionReader));
                        }
                    });
        }
    }

    @Override
    public void release() {
        synchronized (lock) {
            if (isReleased) {
                return;
            }
            isReleased = true;
            allSubpartitionReaders.clear();
        }
        IOUtils.deleteFileQuietly(dataFilePath);
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    private void endCurrentRoundOfReading(int numBuffersRead) {
        synchronized (lock) {
            numRequestedBuffers += numBuffersRead;
            isRunning = false;
        }
        if (numBuffersRead == 0) {
            ioExecutor.schedule(this::mayTriggerReading, 5, TimeUnit.MILLISECONDS);
        } else {
            mayTriggerReading();
        }
    }

    private List<ProducerMergePartitionSubpartitionReader> sortAvailableReaders() {
        synchronized (lock) {
            if (isReleased) {
                return new ArrayList<>();
            }
            List<ProducerMergePartitionSubpartitionReader> availableReaders =
                    new ArrayList<>(allSubpartitionReaders);
            Collections.sort(availableReaders);
            return availableReaders;
        }
    }

    private Queue<MemorySegment> allocateBuffers() throws Exception {
        long timeoutTime = getBufferRequestTimeoutTime();
        do {
            List<MemorySegment> buffers = bufferPool.requestBuffers();
            if (!buffers.isEmpty()) {
                return new ArrayDeque<>(buffers);
            }
            checkState(!isReleased, "Result partition has been already released.");
        } while (System.currentTimeMillis() < timeoutTime
                || System.currentTimeMillis() < (timeoutTime = getBufferRequestTimeoutTime()));
        throw new TimeoutException(
                String.format(
                        "Buffer request timeout, this means there is a fierce contention of"
                                + " the batch shuffle read memory, please increase '%s'.",
                        TaskManagerOptions.NETWORK_BATCH_SHUFFLE_READ_MEMORY.key()));
    }

    private void failSubpartitionReaders(
            Collection<ProducerMergePartitionSubpartitionReader> readers, Throwable failureCause) {
        synchronized (lock) {
            removeSubpartitionReaders(readers);
        }
        for (ProducerMergePartitionSubpartitionReader reader : readers) {
            reader.fail(failureCause);
        }
    }

    private void readData(
            List<ProducerMergePartitionSubpartitionReader> availableReaders,
            Queue<MemorySegment> buffers) {
        int startIndex = 0;
        while (startIndex < availableReaders.size() && !buffers.isEmpty()) {
            ProducerMergePartitionSubpartitionReader subpartitionReader =
                    availableReaders.get(startIndex);
            startIndex++;
            try {
                subpartitionReader.readBuffers(buffers, this);
            } catch (IOException throwable) {
                failSubpartitionReaders(Collections.singletonList(subpartitionReader), throwable);
                LOG.debug("Failed to read shuffle data.", throwable);
            }
        }
    }

    private void releaseBuffers(Queue<MemorySegment> buffers) {
        if (!buffers.isEmpty()) {
            try {
                bufferPool.recycle(buffers);
                buffers.clear();
            } catch (Throwable throwable) {
                // this should never happen so just trigger fatal error
                FatalExitExceptionHandler.INSTANCE.uncaughtException(
                        Thread.currentThread(), throwable);
            }
        }
    }

    private void mayTriggerReading() {
        synchronized (lock) {
            if (!isRunning
                    && !allSubpartitionReaders.isEmpty()
                    && numRequestedBuffers + bufferPool.getNumBuffersPerRequest()
                            <= maxRequestedBuffers
                    && numRequestedBuffers < bufferPool.getAverageBuffersPerRequester()) {
                isRunning = true;
                ioExecutor.execute(
                        () -> {
                            try {
                                run();
                            } catch (Throwable throwable) {
                                LOG.error("Failed to read data.", throwable);
                                // handle un-expected exception as unhandledExceptionHandler is not
                                // worked for ScheduledExecutorService.
                                FatalExitExceptionHandler.INSTANCE.uncaughtException(
                                        Thread.currentThread(), throwable);
                            }
                        });
            }
        }
    }

    private long getBufferRequestTimeoutTime() {
        return bufferPool.getLastBufferOperationTimestamp() + bufferRequestTimeout.toMillis();
    }

    @GuardedBy("lock")
    private void lazyInitialize() throws IOException {
        assert Thread.holdsLock(lock);
        try {
            if (allSubpartitionReaders.isEmpty()) {
                dataFileChannel = openFileChannel(dataFilePath);
                bufferPool.registerRequester(this);
            }
        } catch (IOException exception) {
            if (allSubpartitionReaders.isEmpty()) {
                bufferPool.unregisterRequester(this);
                closeFileChannel();
            }
            throw exception;
        }
    }

    @GuardedBy("lock")
    private void removeSubpartitionReaders(
            Collection<ProducerMergePartitionSubpartitionReader> readers) {
        allSubpartitionReaders.removeAll(readers);
        if (allSubpartitionReaders.isEmpty()) {
            bufferPool.unregisterRequester(this);
            closeFileChannel();
        }
    }

    @GuardedBy("lock")
    private FileChannel openFileChannel(Path path) throws IOException {
        return FileChannel.open(path, StandardOpenOption.READ);
    }

    @GuardedBy("lock")
    private void closeFileChannel() {
        assert Thread.holdsLock(lock);

        IOUtils.closeQuietly(dataFileChannel);
        dataFileChannel = null;
    }

    // ------------------------------------------------------------------------
    //  Implementation Methods of BufferRecycler
    // ------------------------------------------------------------------------

    @Override
    public void recycle(MemorySegment segment) {
        synchronized (lock) {
            bufferPool.recycle(segment);
            --numRequestedBuffers;

            mayTriggerReading();
        }
    }
}
