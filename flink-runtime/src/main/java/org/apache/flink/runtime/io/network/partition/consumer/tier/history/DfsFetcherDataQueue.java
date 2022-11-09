package org.apache.flink.runtime.io.network.partition.consumer.tier.history;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferHeader;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.parseBufferHeader;
import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.throwCorruptDataException;
import static org.apache.flink.runtime.io.network.partition.store.common.StoreReadWriteUtils.getBaseSubpartitionPath;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** The DfsFetcherDataQueue is used to fetch data from DFS. */
public class DfsFetcherDataQueue implements Runnable, FetcherDataQueue {

    private static final Logger LOG = LoggerFactory.getLogger(DfsFetcherDataQueue.class);

    private static final int MINIMUM_BUFFER_NUMBER = 1;

    private static final String SEGMENT_NAME_PREFIX = "/seg-";

    private final AtomicBoolean isSegmentFinished = new AtomicBoolean();

    private final ArrayDeque<MemorySegment> availableBuffers = new ArrayDeque<>();

    private final ArrayDeque<BufferAndAvailability> usedBuffers = new ArrayDeque<>();

    private volatile FetcherDataQueueState currentState = FetcherDataQueueState.CLOSED;

    private BufferPool bufferPool;

    private InputChannel currentChannel;

    private Path currentPath;

    private Map<InputChannel, Long> lastConsumedSegmentIds;

    private int curSequenceNumber = -1;

    private ExecutorService fetcherExecutor;

    private ByteBuffer headerBuffer;

    private JobID jobID;

    List<ResultPartitionID> resultPartitionIDs;

    private int subpartitionIndex;

    private String baseDfsPath;

    @Override
    public void setup(
            JobID jobID,
            List<ResultPartitionID> resultPartitionIDs,
            BufferPool bufferPool,
            int subpartitionIndex,
            String baseDfsPath)
            throws InterruptedException, IOException {
        this.bufferPool = bufferPool;
        headerBuffer = ByteBuffer.wrap(new byte[8]);
        headerBuffer.order(ByteOrder.nativeOrder());
        for (int i = 0; i < MINIMUM_BUFFER_NUMBER; ++i) {
            availableBuffers.add(bufferPool.requestMemorySegmentBlocking());
        }
        this.currentState = FetcherDataQueueState.WAITING;
        this.fetcherExecutor = Executors.newSingleThreadExecutor();
        this.jobID = jobID;
        this.resultPartitionIDs = resultPartitionIDs;
        this.subpartitionIndex = subpartitionIndex;
        this.baseDfsPath = baseDfsPath;
        this.lastConsumedSegmentIds = new HashMap<>();
    }

    @Override
    public void close() throws IOException {
        if (fetcherExecutor != null) {
            fetcherExecutor.shutdown();
        }
        availableBuffers.forEach(MemorySegment::free);
        availableBuffers.clear();
        usedBuffers.clear();
        currentState = FetcherDataQueueState.CLOSED;
    }

    @Override
    public void setSegmentInfo(InputChannel inputChannel, long segmentId)
            throws InterruptedException, IOException {
        checkNotNull(inputChannel);
        checkNotNull(resultPartitionIDs);
        checkState(
                currentState != FetcherDataQueueState.RUNNING,
                "currentState is illegal %s",
                currentState);
        resolveSegmentInfo(segmentId, inputChannel);
        currentChannel = inputChannel;
        synchronized (isSegmentFinished) {
            isSegmentFinished.set(false);
        }
        currentState = FetcherDataQueueState.RUNNING;
        fetcherExecutor.execute(this);
    }

    @Override
    public Optional<BufferAndAvailability> getNextBuffer(Boolean isBlocking)
            throws InterruptedException {
        BufferAndAvailability result;
        while (true) {
            synchronized (isSegmentFinished) {
                result = usedBuffers.poll();
                if (result != null) {
                    break;
                } else {
                    if (isSegmentFinished.get()) {
                        break;
                    } else {
                        isSegmentFinished.wait();
                    }
                }
            }
        }
        return result == null ? Optional.empty() : Optional.of(result);
    }

    @Override
    public FetcherDataQueueState getState() {
        return currentState;
    }

    @Override
    public void setState(FetcherDataQueueState state) {
        currentState = state;
    }

    @Override
    public Optional<InputChannel> getCurrentChannel() {
        return currentChannel == null ? Optional.empty() : Optional.of(currentChannel);
    }

    @Override
    public void run() {
        try (FSDataInputStream currentInputStream = currentPath.getFileSystem().open(currentPath)) {
            while (currentInputStream.available() != 0) {
                BufferAndAvailability bufferAndAvailability = resolveDfsBuffer(currentInputStream);
                if (bufferAndAvailability == null) {
                    continue;
                }
                synchronized (isSegmentFinished) {
                    usedBuffers.add(bufferAndAvailability);
                    isSegmentFinished.notify();
                }
            }
            synchronized (isSegmentFinished) {
                isSegmentFinished.set(true);
                isSegmentFinished.notify();
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    public void resolveSegmentInfo(long segmentId, InputChannel inputChannel) throws IOException {
        checkState(
                segmentId >= 0
                        && segmentId != lastConsumedSegmentIds.getOrDefault(inputChannel, -1L),
                "SegmentId is illegal, previous segmentId: %s, segmentId: %s",
                lastConsumedSegmentIds.getOrDefault(inputChannel, -1L),
                segmentId);
        Path resolvedPath = getResolvedPath(segmentId, inputChannel);
        checkState(isPathExist(resolvedPath));
        currentPath = resolvedPath;
        lastConsumedSegmentIds.put(inputChannel, segmentId);
        curSequenceNumber = 0;
    }

    public Path getResolvedPath(long segmentId, InputChannel inputChannel) {
        boolean isBroadcastOnly = inputChannel.isUpstreamBroadcastOnly();
        String baseSubpartitionPath =
                getBaseSubpartitionPath(
                        jobID,
                        resultPartitionIDs.get(inputChannel.getChannelIndex()),
                        subpartitionIndex,
                        baseDfsPath,
                        isBroadcastOnly);
        return new Path(baseSubpartitionPath, SEGMENT_NAME_PREFIX + segmentId);
    }

    public boolean isPathExist(Path path) {
        try {
            return path.getFileSystem().exists(path);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to check the existing state of segment path:" + this.currentPath);
        }
    }

    public BufferAndAvailability resolveDfsBuffer(FSDataInputStream inputStream)
            throws InterruptedException, IOException {
        LOG.debug("### START RUNNING! resolve1 " + currentPath);
        if (availableBuffers.isEmpty()) {
            LOG.debug("### START RUNNING! resolve2 " + currentPath);
            MemorySegment memorySegment = bufferPool.requestMemorySegmentBlocking();
            if (memorySegment != null) {
                availableBuffers.add(memorySegment);
            }
        }
        if (availableBuffers.isEmpty()) {
            return null;
        }
        LOG.debug("### START RUNNING! resolve3 " + currentPath);
        MemorySegment memorySegment = availableBuffers.poll();
        LOG.debug("### START RUNNING! resolve4 " + currentPath);
        Buffer buffer = checkNotNull(readFromInputStream(memorySegment, inputStream));
        return new BufferAndAvailability(
                buffer, Buffer.DataType.DATA_BUFFER, 0, curSequenceNumber++);
    }

    private Buffer readFromInputStream(MemorySegment memorySegment, FSDataInputStream inputStream)
            throws IOException {
        headerBuffer.clear();
        int bufferHeaderResult = inputStream.read(headerBuffer.array());
        if (bufferHeaderResult == -1) {
            return null;
        }
        final BufferHeader header;
        try {
            header = parseBufferHeader(headerBuffer);
        } catch (BufferUnderflowException | IllegalArgumentException e) {
            // buffer underflow if header buffer is undersized
            // IllegalArgumentException if size is outside memory segment size
            throwCorruptDataException();
            return null; // silence compiler
        }
        ByteBuffer dataBuffer = ByteBuffer.wrap(new byte[header.getLength()]);
        int dataBufferResult = inputStream.read(dataBuffer.array(), 0, header.getLength());
        if (dataBufferResult == -1) {
            return null;
        }
        Buffer.DataType dataType = header.getDataType();
        memorySegment.put(0, dataBuffer.array(), 0, header.getLength());
        return new NetworkBuffer(
                memorySegment, bufferPool, dataType, header.isCompressed(), header.getLength());
    }

    @Override
    public boolean isSegmentExist(long segmentId, InputChannel inputChannel) {
        Path resolvedPath = getResolvedPath(segmentId, inputChannel);
        return isPathExist(resolvedPath);
    }

    @Override
    public long getCurrentSegmentId(InputChannel inputChannel) {
        return lastConsumedSegmentIds.getOrDefault(inputChannel, -1L);
    }

    @VisibleForTesting
    public Path getCurrentDataPath() {
        return currentPath;
    }

    @VisibleForTesting
    public int getCurSequenceNumber() {
        return curSequenceNumber;
    }

    @VisibleForTesting
    public ArrayDeque<MemorySegment> getAvailableBuffers() {
        return availableBuffers;
    }

    @VisibleForTesting
    public ArrayDeque<BufferAndAvailability> getUsedBuffers() {
        return usedBuffers;
    }
}
