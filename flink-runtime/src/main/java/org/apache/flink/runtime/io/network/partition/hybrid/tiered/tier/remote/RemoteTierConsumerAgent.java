package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.remote;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.BufferHeader;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyServiceReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierConsumerAgent;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Optional;

import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.HEADER_LENGTH;
import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.parseBufferHeader;

/** The data client is used to fetch data from DFS tier. */
public class RemoteTierConsumerAgent implements TierConsumerAgent {

    private final int[] requiredSegmentIds;

    private final RemoteTierMonitor remoteTierMonitor;

    private final TieredStorageMemoryManager storageMemoryManager;

    private final NettyServiceReader consumerNettyService;

    private final ByteBuffer headerBuffer;

    public RemoteTierConsumerAgent(
            int numInputChannels,
            TieredStorageMemoryManager storageMemoryManager,
            RemoteTierMonitor remoteTierMonitor,
            NettyServiceReader consumerNettyService) {
        this.remoteTierMonitor = remoteTierMonitor;
        this.storageMemoryManager = storageMemoryManager;
        this.consumerNettyService = consumerNettyService;
        this.requiredSegmentIds = new int[numInputChannels];
        Arrays.fill(requiredSegmentIds, -1);
        this.headerBuffer = ByteBuffer.wrap(new byte[HEADER_LENGTH]);
        headerBuffer.order(ByteOrder.nativeOrder());
    }

    @Override
    public void start() {
        remoteTierMonitor.start();
    }

    @Override
    public Optional<Buffer> getNextBuffer(int subpartitionId, int segmentId) {
        if (segmentId != requiredSegmentIds[subpartitionId]) {
            remoteTierMonitor.updateRequiredSegmentId(subpartitionId, segmentId);
            requiredSegmentIds[subpartitionId] = segmentId;
        }
        if (!remoteTierMonitor.isExist(subpartitionId, segmentId)) {
            return Optional.empty();
        }
        InputStream currentInputStream =
                remoteTierMonitor.getSegmentFileInputStream(subpartitionId, segmentId);
        try {
            if (currentInputStream.available() == 0) {
                currentInputStream.close();
                return Optional.of(
                        new NetworkBuffer(
                                MemorySegmentFactory.allocateUnpooledSegment(0),
                                FreeingBufferRecycler.INSTANCE,
                                Buffer.DataType.END_OF_SEGMENT,
                                0));
            } else {
                consumerNettyService.notifyResultSubpartitionAvailable(subpartitionId, false);
                return Optional.of(readBuffer(currentInputStream));
            }
        } catch (IOException e) {
            ExceptionUtils.rethrow(e, "Failed to get next buffer in remote consumer agent");
        }
        return Optional.empty();
    }

    @Override
    public void close() throws IOException {
        remoteTierMonitor.close();
    }

    // ------------------------------------
    //           Internal Method
    // ------------------------------------

    private Buffer readBuffer(InputStream inputStream) throws IOException {
        headerBuffer.clear();
        int bufferHeaderResult = inputStream.read(headerBuffer.array());
        if (bufferHeaderResult == -1) {
            throw new IOException("Empty header buffer is read from dfs.");
        }
        BufferHeader header = parseBufferHeader(headerBuffer);
        ByteBuffer dataBuffer = ByteBuffer.wrap(new byte[header.getLength()]);
        int dataBufferResult = inputStream.read(dataBuffer.array(), 0, header.getLength());
        if (dataBufferResult == -1) {
            throw new IOException("Empty data buffer is read from dfs.");
        }
        Buffer.DataType dataType = header.getDataType();
        if (dataType.isBuffer()) {
            BufferBuilder builder = storageMemoryManager.requestBufferBlocking(this);
            BufferConsumer bufferConsumer = builder.createBufferConsumer();
            Buffer buffer = bufferConsumer.build();
            MemorySegment memorySegment = buffer.getMemorySegment();
            memorySegment.put(0, dataBuffer.array(), 0, header.getLength());
            return new NetworkBuffer(
                    memorySegment,
                    buffer.getRecycler(),
                    dataType,
                    header.isCompressed(),
                    header.getLength());
        } else {
            MemorySegment memorySegment = MemorySegmentFactory.wrap(dataBuffer.array());
            return new NetworkBuffer(
                    memorySegment, FreeingBufferRecycler.INSTANCE, dataType, memorySegment.size());
        }
    }
}
