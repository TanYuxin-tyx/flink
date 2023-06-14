package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.ioscheduler;

import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyServiceProducer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file.FileReaderId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file.PartitionFileManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file.PartitionFileReader;
import org.apache.flink.util.FatalExitExceptionHandler;
import org.apache.flink.util.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.file.PartitionFileType.PRODUCER_MERGE;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link DiskIOScheduler} is the scheduler to control the reading of data from shuffle file, which
 * ensures the order of buffers in each subpartition is correct. The buffers will be read from file
 * and sent to netty server through {@link NettyConnectionWriter}.
 */
public class DiskIOScheduler implements Runnable, BufferRecycler, NettyServiceProducer {

    private static final Logger LOG = LoggerFactory.getLogger(DiskIOScheduler.class);

    private final ScheduledExecutorService ioExecutor;

    private final Duration bufferRequestTimeout;

    private final Object lock = new Object();

    private final BatchShuffleReadBufferPool bufferPool;

    private final Path dataFilePath;

    private final int maxBufferReadAhead;

    private final int maxRequestedBuffers;

    private final TieredStorageNettyService nettyService;

    private final List<Map<Integer, Integer>> firstBufferContextInSegment;

    private final Map<NettyConnectionId, FileReaderId> allFileReaderIds = new ConcurrentHashMap<>();

    private final PartitionFileReader partitionFileReader;

    @GuardedBy("lock")
    private final Map<FileReaderId, ScheduledSubpartition> allScheduledSubpartitions =
            new HashMap<>();

    @GuardedBy("lock")
    private FileChannel dataFileChannel;

    @GuardedBy("lock")
    private volatile boolean isRunning;

    @GuardedBy("lock")
    private volatile int numRequestedBuffers;

    @GuardedBy("lock")
    private volatile boolean isReleased;

    public DiskIOScheduler(
            BatchShuffleReadBufferPool bufferPool,
            ScheduledExecutorService ioExecutor,
            Path dataFilePath,
            int maxRequestedBuffers,
            Duration bufferRequestTimeout,
            int maxBufferReadAhead,
            TieredStorageNettyService nettyService,
            List<Map<Integer, Integer>> firstBufferContextInSegment,
            PartitionFileManager partitionFileManager) {
        this.dataFilePath = checkNotNull(dataFilePath);
        this.bufferPool = checkNotNull(bufferPool);
        this.ioExecutor = checkNotNull(ioExecutor);
        this.maxRequestedBuffers = maxRequestedBuffers;
        this.bufferRequestTimeout = checkNotNull(bufferRequestTimeout);
        this.maxBufferReadAhead = maxBufferReadAhead;
        this.nettyService = nettyService;
        this.firstBufferContextInSegment = firstBufferContextInSegment;
        this.partitionFileReader = partitionFileManager.createPartitionFileReader(PRODUCER_MERGE);
    }

    @Override
    public synchronized void run() {
        int numBuffersRead = readBuffersFromFile();
        isRunning = false;
        if (numBuffersRead == 0) {
            ioExecutor.schedule(this::triggerReaderRunning, 5, TimeUnit.MILLISECONDS);
        } else {
            triggerReaderRunning();
        }
    }

    @Override
    public void connectionEstablished(
            TieredStorageSubpartitionId subpartitionId,
            NettyConnectionWriter nettyConnectionWriter) {
        synchronized (lock) {
            checkState(!isReleased, "ProducerMergePartitionFileReader is already released.");
            lazyInitialize();
            ScheduledSubpartition scheduledSubpartition =
                    new ScheduledSubpartition(
                            subpartitionId.getSubpartitionId(),
                            maxBufferReadAhead,
                            nettyConnectionWriter,
                            nettyService,
                            firstBufferContextInSegment.get(subpartitionId.getSubpartitionId()),
                            partitionFileReader);
            allScheduledSubpartitions.put(
                    scheduledSubpartition.getFileReaderId(), scheduledSubpartition);
            allFileReaderIds.put(
                    nettyConnectionWriter.getNettyConnectionId(),
                    scheduledSubpartition.getFileReaderId());
            triggerReaderRunning();
        }
    }

    @Override
    public void connectionBroken(NettyConnectionId id) {
        removeSubpartition(allFileReaderIds.get(id));
    }

    public void release() {
        synchronized (lock) {
            if (isReleased) {
                return;
            }
            isReleased = true;
            allScheduledSubpartitions.clear();
            firstBufferContextInSegment.clear();
        }
        IOUtils.deleteFileQuietly(dataFilePath);
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    @GuardedBy("lock")
    private void lazyInitialize() {
        assert Thread.holdsLock(lock);
        // try {
        //    if (allScheduledSubpartitions.isEmpty()) {
        //        dataFileChannel = openFileChannel(dataFilePath);
        //        bufferPool.registerRequester(this);
        //    }
        // } catch (IOException e) {
        //    if (allScheduledSubpartitions.isEmpty()) {
        //        bufferPool.unregisterRequester(this);
        //        closeFileChannel();
        //    }
        //    throw new RuntimeException(e);
        // }
        if (allScheduledSubpartitions.isEmpty()) {
            bufferPool.registerRequester(this);
        }
    }

    private int readBuffersFromFile() {
        List<ScheduledSubpartition> availableReaders = sortAvailableReaders();
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
        synchronized (lock) {
            numRequestedBuffers += numBuffersRead;
        }
        return numBuffersRead;
    }

    private List<ScheduledSubpartition> sortAvailableReaders() {
        synchronized (lock) {
            if (isReleased) {
                return new ArrayList<>();
            }
            List<ScheduledSubpartition> availableReaders =
                    new ArrayList<>(allScheduledSubpartitions.values());
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
            synchronized (lock) {
                if (isReleased) {
                    return new ArrayDeque<>();
                }
            }
        } while (System.currentTimeMillis() < timeoutTime
                || System.currentTimeMillis() < (timeoutTime = getBufferRequestTimeoutTime()));
        throw new TimeoutException(
                String.format(
                        "Buffer request timeout, this means there is a fierce contention of"
                                + " the batch shuffle read memory, please increase '%s'.",
                        TaskManagerOptions.NETWORK_BATCH_SHUFFLE_READ_MEMORY.key()));
    }

    private void failSubpartitionReaders(
            Collection<ScheduledSubpartition> subpartitionReaders, Throwable failureCause) {
        for (ScheduledSubpartition subpartitionReader : subpartitionReaders) {
            removeSubpartition(subpartitionReader.getFileReaderId());
            subpartitionReader.fail(failureCause);
        }
    }

    private void readData(
            List<ScheduledSubpartition> availableReaders, Queue<MemorySegment> buffers) {
        int startIndex = 0;
        while (startIndex < availableReaders.size() && !buffers.isEmpty()) {
            ScheduledSubpartition subpartitionReader = availableReaders.get(startIndex);
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

    private void triggerReaderRunning() {
        synchronized (lock) {
            if (!isRunning
                    && !allScheduledSubpartitions.isEmpty()
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

    private void removeSubpartition(FileReaderId readerId) {
        synchronized (lock) {
            allScheduledSubpartitions.remove(readerId);
            if (allScheduledSubpartitions.isEmpty()) {
                bufferPool.unregisterRequester(this);
                closeFileChannel();
            }
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
            triggerReaderRunning();
        }
    }
}
