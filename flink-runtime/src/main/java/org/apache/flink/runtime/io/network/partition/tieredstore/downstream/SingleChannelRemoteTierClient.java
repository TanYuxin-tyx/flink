package org.apache.flink.runtime.io.network.partition.tieredstore.downstream;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.api.EndOfSegmentEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferHeader;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.LocalRecoveredInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteRecoveredInputChannel;
import org.apache.flink.runtime.io.network.partition.tieredstore.downstream.common.SingleChannelTierClient;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.TieredStoreMode;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreMemoryManager;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.HEADER_LENGTH;
import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.parseBufferHeader;
import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.throwCorruptDataException;
import static org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreUtils.generateNewSegmentPath;
import static org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreUtils.generateSegmentFinishPath;
import static org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreUtils.getBaseSubpartitionPath;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** The data client is used to fetch data from DFS tier. */
public class SingleChannelRemoteTierClient implements SingleChannelTierClient {

    private final TieredStoreMemoryManager memoryManager;

    private final ByteBuffer headerBuffer;

    private final List<ResultPartitionID> resultPartitionIDs;

    private final JobID jobID;

    private final List<Integer> subpartitionIndexes;

    private final String baseRemoteStoragePath;

    private final FileSystem remoteFileSystem;

    private Path currentPath;

    private Path currentSegmentFinishPath;

    private FSDataInputStream currentInputStream;

    private int lastestSegmentId = -1;

    public SingleChannelRemoteTierClient(
            JobID jobID,
            List<ResultPartitionID> resultPartitionIDs,
            List<Integer> subpartitionIndexes,
            TieredStoreMemoryManager memoryManager,
            String baseRemoteStoragePath) {
        this.resultPartitionIDs = resultPartitionIDs;
        this.jobID = jobID;
        this.headerBuffer = ByteBuffer.wrap(new byte[HEADER_LENGTH]);
        headerBuffer.order(ByteOrder.nativeOrder());
        this.subpartitionIndexes = subpartitionIndexes;
        this.memoryManager = memoryManager;
        this.baseRemoteStoragePath = baseRemoteStoragePath;
        try {
            this.remoteFileSystem = new Path(baseRemoteStoragePath).getFileSystem();
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize the FileSystem", e);
        }
    }

    @Override
    public Optional<InputChannel.BufferAndAvailability> getNextBuffer(
            InputChannel inputChannel, int segmentId) throws IOException {
        if (inputChannel.getClass() == RemoteRecoveredInputChannel.class
                || inputChannel.getClass() == LocalRecoveredInputChannel.class) {
            return Optional.empty();
        }

        if (!hasSegmentId(inputChannel, segmentId)) {
            return Optional.empty();
        }
        checkState(currentInputStream != null, "CurrentInputStream must not be null.");
        if (currentInputStream.available() == 0) {
            return Optional.of(
                    new InputChannel.BufferAndAvailability(
                            buildEndOfSegmentBuffer(lastestSegmentId + 1),
                            Buffer.DataType.SEGMENT_EVENT,
                            0,
                            0));
        } else {
            return Optional.of(getDfsBuffer(currentInputStream));
        }
    }

    @Override
    public void close() throws IOException {
        if (currentInputStream != null) {
            currentInputStream.close();
        }
    }

    // ------------------------------------
    //           Internal Method
    // ------------------------------------

    private boolean hasSegmentId(InputChannel inputChannel, int segmentId) throws IOException {
        if (segmentId != lastestSegmentId) {
            lastestSegmentId = segmentId;
            boolean isBroadcastOnly = inputChannel.isUpstreamBroadcastOnly();
            String baseSubpartitionPath =
                    getBaseSubpartitionPath(
                            jobID,
                            resultPartitionIDs.get(inputChannel.getChannelIndex()),
                            subpartitionIndexes.get(inputChannel.getChannelIndex()),
                            baseRemoteStoragePath,
                            isBroadcastOnly);
            currentPath = generateNewSegmentPath(baseSubpartitionPath, segmentId);
            currentSegmentFinishPath = generateSegmentFinishPath(baseSubpartitionPath, segmentId);
            if (currentInputStream != null) {
                currentInputStream.close();
                currentInputStream = null;
            }
        }
        if (isPathExist(currentSegmentFinishPath)) {
            checkState(isPathExist(currentPath), "Empty Segment data file path.");
            if (currentInputStream == null) {
                currentInputStream = remoteFileSystem.open(currentPath);
            }
            return true;
        } else {
            return false;
        }
    }

    private boolean isPathExist(Path path) {
        try {
            return remoteFileSystem.exists(path);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to check the existing state of segment path:" + this.currentPath);
        }
    }

    private InputChannel.BufferAndAvailability getDfsBuffer(FSDataInputStream inputStream)
            throws IOException {
        MemorySegment memorySegment =
                memoryManager.requestMemorySegmentBlocking(TieredStoreMode.TieredType.IN_DFS);
        Buffer buffer = checkNotNull(readFromInputStream(memorySegment, inputStream));
        return new InputChannel.BufferAndAvailability(buffer, Buffer.DataType.DATA_BUFFER, 0, 0);
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
                memorySegment, this::recycle, dataType, header.isCompressed(), header.getLength());
    }

    private Buffer buildEndOfSegmentBuffer(int segmentId) throws IOException {
        MemorySegment data =
                MemorySegmentFactory.wrap(
                        EventSerializer.toSerializedEvent(new EndOfSegmentEvent(segmentId))
                                .array());
        return new NetworkBuffer(
                data, FreeingBufferRecycler.INSTANCE, Buffer.DataType.SEGMENT_EVENT, data.size());
    }

    private void recycle(MemorySegment memorySegment) {
        memoryManager.recycleBuffer(memorySegment, TieredStoreMode.TieredType.IN_DFS);
    }

    @VisibleForTesting
    public int getLatestSegmentId() {
        return lastestSegmentId;
    }
}
