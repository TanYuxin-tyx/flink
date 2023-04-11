package org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.file;

import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.TieredStoreConfiguration;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.NettyBasedTierConsumer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.NettyBasedTierConsumerView;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.NettyBasedTierConsumerViewId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.tier.local.disk.RegionBufferIndexTracker;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** THe implementation of {@link PartitionFileReader} with merged logic. */
public class ProducerMergePartitionFileReader
        implements Runnable, BufferRecycler, PartitionFileReader {

    private static final Logger LOG =
            LoggerFactory.getLogger(ProducerMergePartitionFileReader.class);

    /** Executor to run the shuffle data reading task. */
    private final ScheduledExecutorService ioExecutor;

    /** Maximum number of buffers can be allocated by this partition reader. */
    private final int maxRequestedBuffers;

    /**
     * Maximum time to wait when requesting read buffers from the buffer pool before throwing an
     * exception.
     */
    private final Duration bufferRequestTimeout;

    /** Lock used to synchronize multi-thread access to thread-unsafe fields. */
    private final Object lock = new Object();

    /**
     * A {@link CompletableFuture} to be completed when this data manager including all resources is
     * released.
     */
    @GuardedBy("lock")
    private final CompletableFuture<?> releaseFuture = new CompletableFuture<>();

    /** Buffer pool from which to allocate buffers for shuffle data reading. */
    private final BatchShuffleReadBufferPool bufferPool;

    private final Path dataFilePath;

    private final RegionBufferIndexTracker dataIndex;

    private final ProducerMergePartitionTierConsumer.Factory fileReaderFactory;

    private final TieredStoreConfiguration storeConfiguration;

    private final ByteBuffer headerBuf = BufferReaderWriterUtil.allocatedHeaderBuffer();

    /** All readers waiting to read data of different subpartitions. */
    @GuardedBy("lock")
    private final Set<ProducerMergePartitionTierConsumer> allReaders = new HashSet<>();

    /**
     * Whether the data reading task is currently running or not. This flag is used when trying to
     * submit the data reading task.
     */
    @GuardedBy("lock")
    private boolean isRunning;

    /** Number of buffers already allocated and still not recycled by this partition reader. */
    @GuardedBy("lock")
    private volatile int numRequestedBuffers;

    /** Whether this file data manager has been released or not. */
    @GuardedBy("lock")
    private volatile boolean isReleased;

    @GuardedBy("lock")
    private FileChannel dataFileChannel;

    public ProducerMergePartitionFileReader(
            BatchShuffleReadBufferPool bufferPool,
            ScheduledExecutorService ioExecutor,
            RegionBufferIndexTracker dataIndex,
            Path dataFilePath,
            ProducerMergePartitionTierConsumer.Factory fileReaderFactory,
            TieredStoreConfiguration storeConfiguration) {
        this.fileReaderFactory = fileReaderFactory;
        this.storeConfiguration = checkNotNull(storeConfiguration);
        this.dataIndex = checkNotNull(dataIndex);
        this.dataFilePath = checkNotNull(dataFilePath);
        this.bufferPool = checkNotNull(bufferPool);
        this.ioExecutor = checkNotNull(ioExecutor);
        this.maxRequestedBuffers = storeConfiguration.getMaxRequestedBuffers();
        this.bufferRequestTimeout = checkNotNull(storeConfiguration.getBufferRequestTimeout());
    }

    /** Setup read buffer pool. */
    public void setup() {
        bufferPool.initialize();
    }

    @Override
    // Note, this method is synchronized on `this`, not `lock`. The purpose here is to prevent
    // concurrent `run()` executions. Concurrent calls to other methods are allowed.
    public synchronized void run() {
        int numBuffersRead = tryRead();
        endCurrentRoundOfReading(numBuffersRead);
    }

    /** This method only called by result partition to create subpartitionFileReader. */
    public NettyBasedTierConsumer registerTierReader(
            int subpartitionId,
            NettyBasedTierConsumerViewId nettyBasedTierConsumerViewId,
            NettyBasedTierConsumerView tierConsumerView)
            throws IOException {
        synchronized (lock) {
            checkState(!isReleased, "HsFileDataManager is already released.");
            lazyInitialize();

            ProducerMergePartitionTierConsumer subpartitionReader =
                    fileReaderFactory.createFileReader(
                            subpartitionId,
                            nettyBasedTierConsumerViewId,
                            dataFileChannel,
                            tierConsumerView,
                            dataIndex,
                            storeConfiguration.getMaxBuffersReadAhead(),
                            this::releaseSubpartitionReader,
                            headerBuf);

            allReaders.add(subpartitionReader);

            mayTriggerReading();
            return subpartitionReader;
        }
    }

    public void deleteShuffleFile() {
        IOUtils.deleteFileQuietly(dataFilePath);
    }

    /**
     * Release specific {@link ProducerMergePartitionTierConsumer} from {@link PartitionFileReader}.
     *
     * @param producerMergePartitionTierReaderView to release.
     */
    public void releaseSubpartitionReader(
            ProducerMergePartitionTierConsumer producerMergePartitionTierReaderView) {
        synchronized (lock) {
            removeSubpartitionReaders(Collections.singleton(producerMergePartitionTierReaderView));
        }
    }

    /** Releases this file data manager and delete shuffle data after all readers is removed. */
    public void release() {
        synchronized (lock) {
            if (isReleased) {
                return;
            }
            isReleased = true;
            releaseFuture.complete(null);
            allReaders.clear();
        }
        releaseFuture.thenRunAsync(this::deleteShuffleFile);
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    /** @return number of buffers read. */
    private int tryRead() {
        List<ProducerMergePartitionTierConsumer> availableReaders = prepareAndGetAvailableReaders();
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

    @SuppressWarnings("FieldAccessNotGuarded")
    // read-only access to volatile isReleased and numRequestedBuffers
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

        // This is a safe net against potential deadlocks.
        //
        // A deadlock can happen when the downstream task needs to consume multiple result
        // partitions (e.g., A and B) in specific order (cannot consume B before finishing
        // consuming A). Since the reading buffer pool is shared across the TM, if B happens to
        // take all the buffers, A cannot be consumed due to lack of buffers, which also blocks
        // B from being consumed and releasing the buffers.
        //
        // The imperfect solution here is to fail all the subpartitionReaders (A), which
        // consequently fail all the downstream tasks, unregister their other
        // subpartitionReaders (B) and release the read buffers.
        throw new TimeoutException(
                String.format(
                        "Buffer request timeout, this means there is a fierce contention of"
                                + " the batch shuffle read memory, please increase '%s'.",
                        TaskManagerOptions.NETWORK_BATCH_SHUFFLE_READ_MEMORY.key()));
    }

    private void mayTriggerReading() {
        synchronized (lock) {
            if (!isRunning
                    && !allReaders.isEmpty()
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

    @GuardedBy("lock")
    private void mayNotifyReleased() {
        assert Thread.holdsLock(lock);

        if (isReleased && allReaders.isEmpty()) {
            releaseFuture.complete(null);
        }
    }

    private long getBufferRequestTimeoutTime() {
        return bufferPool.getLastBufferOperationTimestamp() + bufferRequestTimeout.toMillis();
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

    private List<ProducerMergePartitionTierConsumer> prepareAndGetAvailableReaders() {
        synchronized (lock) {
            if (isReleased) {
                return new ArrayList<>();
            }
            List<ProducerMergePartitionTierConsumer> availableReaders = new ArrayList<>();
            for (ProducerMergePartitionTierConsumer reader : allReaders) {
                reader.prepareForScheduling();
                availableReaders.add(reader);
            }
            Collections.sort(availableReaders);
            return availableReaders;
        }
    }

    private void readData(
            List<ProducerMergePartitionTierConsumer> availableReaders,
            Queue<MemorySegment> buffers) {
        int startIndex = 0;
        while (startIndex < availableReaders.size() && !buffers.isEmpty()) {
            ProducerMergePartitionTierConsumer subpartitionReader =
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

    private void failSubpartitionReaders(
            Collection<ProducerMergePartitionTierConsumer> readers, Throwable failureCause) {
        synchronized (lock) {
            removeSubpartitionReaders(readers);
        }

        for (ProducerMergePartitionTierConsumer reader : readers) {
            reader.fail(failureCause);
        }
    }

    @GuardedBy("lock")
    private void removeSubpartitionReaders(Collection<ProducerMergePartitionTierConsumer> readers) {
        allReaders.removeAll(readers);
        if (allReaders.isEmpty()) {
            bufferPool.unregisterRequester(this);
            closeFileChannel();
        }
    }

    private void endCurrentRoundOfReading(int numBuffersRead) {
        synchronized (lock) {
            numRequestedBuffers += numBuffersRead;
            isRunning = false;
            mayNotifyReleased();
        }
        if (numBuffersRead == 0) {
            // When fileReader has no data to read, for example, most of the data is
            // consumed from memory. HsFileDataManager will encounter busy-loop
            // problem, which will lead to a meaningless surge in CPU utilization
            // and seriously affect performance.
            ioExecutor.schedule(this::mayTriggerReading, 5, TimeUnit.MILLISECONDS);
        } else {
            mayTriggerReading();
        }
    }

    @GuardedBy("lock")
    private void lazyInitialize() throws IOException {
        assert Thread.holdsLock(lock);
        try {
            if (allReaders.isEmpty()) {
                dataFileChannel = openFileChannel(dataFilePath);
                bufferPool.registerRequester(this);
            }
        } catch (IOException exception) {
            if (allReaders.isEmpty()) {
                bufferPool.unregisterRequester(this);
                closeFileChannel();
            }
            throw exception;
        }
    }

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