package org.apache.flink.runtime.io.network.partition.tieredstore.downstream;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.api.EndOfSegmentEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferHeader;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.LocalRecoveredInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteRecoveredInputChannel;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.TieredStoreMode;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreMemoryManager;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Optional;

import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.HEADER_LENGTH;
import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.parseBufferHeader;
import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.throwCorruptDataException;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** The data client is used to fetch data from DFS tier. */
public class RemoteTierClient implements TierClient {

    private final TieredStoreMemoryManager memoryManager;

    private final ByteBuffer headerBuffer;

    private final RemoteTierMonitor remoteTierMonitor;

    private FSDataInputStream currentInputStream;

    private int latestSegmentId = -1;

    public RemoteTierClient(
            TieredStoreMemoryManager memoryManager, RemoteTierMonitor remoteTierMonitor) {
        this.headerBuffer = ByteBuffer.wrap(new byte[HEADER_LENGTH]);
        headerBuffer.order(ByteOrder.nativeOrder());
        this.memoryManager = memoryManager;
        this.remoteTierMonitor = remoteTierMonitor;
    }

    @Override
    public Optional<InputChannel.BufferAndAvailability> getNextBuffer(
            InputChannel inputChannel, int segmentId) throws IOException {
        try {
            if (inputChannel.getClass() == LocalRecoveredInputChannel.class
                    || inputChannel.getClass() == RemoteRecoveredInputChannel.class) {
                return inputChannel.getNextBuffer();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (segmentId != latestSegmentId) {
            remoteTierMonitor.requireSegmentId(inputChannel.getChannelIndex(), segmentId);
            latestSegmentId = segmentId;
        }
        if (!remoteTierMonitor.isExist(inputChannel.getChannelIndex(), segmentId)) {
            return Optional.empty();
        }
        currentInputStream = remoteTierMonitor.getInputStream(inputChannel.getChannelIndex());
        if (currentInputStream.available() == 0) {
            currentInputStream.close();
            return Optional.of(
                    new InputChannel.BufferAndAvailability(
                            buildEndOfSegmentBuffer(latestSegmentId + 1),
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
        remoteTierMonitor.close();
    }

    // ------------------------------------
    //           Internal Method
    // ------------------------------------

    private InputChannel.BufferAndAvailability getDfsBuffer(FSDataInputStream inputStream)
            throws IOException {
        MemorySegment memorySegment =
                memoryManager.requestMemorySegmentBlocking(TieredStoreMode.TierType.IN_REMOTE);
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
        memoryManager.recycleBuffer(memorySegment, TieredStoreMode.TierType.IN_REMOTE);
    }

    @VisibleForTesting
    public int getLatestSegmentId() {
        return latestSegmentId;
    }
}
