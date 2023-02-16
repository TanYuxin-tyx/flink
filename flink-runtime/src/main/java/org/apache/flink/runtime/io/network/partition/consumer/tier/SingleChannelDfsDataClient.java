package org.apache.flink.runtime.io.network.partition.consumer.tier;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.api.EndOfSegmentEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferHeader;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.tier.common.SingleChannelDataClient;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.parseBufferHeader;
import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.throwCorruptDataException;
import static org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreUtils.getBaseSubpartitionPath;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** The data client is used to fetch data from DFS tier. */
public class SingleChannelDfsDataClient implements SingleChannelDataClient {

    private static final String SEGMENT_NAME_PREFIX = "/seg-";

    private final NetworkBufferPool networkBufferPool;

    private final ByteBuffer headerBuffer;

    private final List<ResultPartitionID> resultPartitionIDs;

    private final JobID jobID;

    private final int subpartitionIndex;

    private final String baseDfsPath;

    private Path currentPath;

    private FSDataInputStream currentInputStream;

    private long currentSegmentId = -1L;

    public SingleChannelDfsDataClient(
            JobID jobID,
            List<ResultPartitionID> resultPartitionIDs,
            int subpartitionIndex,
            NetworkBufferPool networkBufferPool,
            String baseDfsPath) {
        this.resultPartitionIDs = resultPartitionIDs;
        this.jobID = jobID;
        this.headerBuffer = ByteBuffer.wrap(new byte[8]);
        headerBuffer.order(ByteOrder.nativeOrder());
        this.subpartitionIndex = subpartitionIndex;
        this.networkBufferPool = networkBufferPool;
        this.baseDfsPath = baseDfsPath;
    }

    @Override
    public boolean hasSegmentId(InputChannel inputChannel, long segmentId) throws IOException {
        if (segmentId != currentSegmentId) {
            currentSegmentId = segmentId;
            currentPath = getDfsPath(inputChannel, segmentId);
            if (currentInputStream != null) {
                currentInputStream.close();
                currentInputStream = null;
            }
        }
        if (isPathExist(currentPath)) {
            if (currentInputStream == null) {
                currentInputStream = currentPath.getFileSystem().open(currentPath);
            }
            return true;
        } else {
            return false;
        }
    }

    @Override
    public Optional<InputChannel.BufferAndAvailability> getNextBuffer(
            InputChannel inputChannel, long segmentId) throws IOException {
        checkState(
                segmentId == currentSegmentId,
                "SegmentId is illegal, currentSegmentId: %s, segmentId: %s",
                currentSegmentId,
                segmentId);
        checkState(currentInputStream != null, "CurrentInputStream must not be null.");
        if (currentInputStream.available() == 0) {
            return Optional.of(
                    new InputChannel.BufferAndAvailability(
                            buildEndOfSegmentBuffer(currentSegmentId + 1),
                            Buffer.DataType.SEGMENT_EVENT,
                            0,
                            0));
        } else {
            return Optional.of(getDfsBuffer(currentInputStream));
        }
    }

    @Override
    public void close() throws IOException {}

    // ------------------------------------
    //           Internal Method
    // ------------------------------------

    public Path getDfsPath(InputChannel inputChannel, long segmentId) {
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

    private boolean isPathExist(Path path) {
        try {
            return path.getFileSystem().exists(path);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to check the existing state of segment path:" + this.currentPath);
        }
    }

    private InputChannel.BufferAndAvailability getDfsBuffer(FSDataInputStream inputStream)
            throws IOException {
        MemorySegment memorySegment = getSingleMemorySegment();
        Buffer buffer = checkNotNull(readFromInputStream(memorySegment, inputStream));
        return new InputChannel.BufferAndAvailability(buffer, Buffer.DataType.DATA_BUFFER, 0, 0);
    }

    private MemorySegment getSingleMemorySegment() throws IOException {
        List<MemorySegment> memorySegments = networkBufferPool.requestUnpooledMemorySegments(1);
        return memorySegments.get(0);
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

    private Buffer buildEndOfSegmentBuffer(long segmentId) throws IOException {
        MemorySegment data =
                MemorySegmentFactory.wrap(
                        EventSerializer.toSerializedEvent(new EndOfSegmentEvent(segmentId))
                                .array());
        return new NetworkBuffer(
                data, FreeingBufferRecycler.INSTANCE, Buffer.DataType.SEGMENT_EVENT, data.size());
    }

    private void recycle(MemorySegment memorySegment) {
        networkBufferPool.recycleUnpooledMemorySegments(Collections.singletonList(memorySegment));
    }
}
