package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.remote;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferHeader;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierConsumerAgent;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;

import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.HEADER_LENGTH;
import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.parseBufferHeader;

/** The data client is used to fetch data from DFS tier. */
public class RemoteTierConsumerAgent implements TierConsumerAgent {

    private final int[] requiredSegmentIds;

    private final RemoteTierMonitor remoteTierMonitor;

    private final TieredStorageMemoryManager storageMemoryManager;

    private final BiConsumer<Integer, Boolean> queueChannelCallBack;

    private final ByteBuffer headerBuffer;

    private final Map<TieredStoragePartitionId, Map<TieredStorageSubpartitionId, Integer>>
            subpartitionIndexs = new HashMap<>();

    public RemoteTierConsumerAgent(
            List<Tuple2<TieredStoragePartitionId, TieredStorageSubpartitionId>>
                    partitionIdAndSubpartitionIds,
            TieredStorageMemoryManager storageMemoryManager,
            RemoteTierMonitor remoteTierMonitor,
            BiConsumer<Integer, Boolean> queueChannelCallBack) {
        this.remoteTierMonitor = remoteTierMonitor;
        this.storageMemoryManager = storageMemoryManager;
        this.queueChannelCallBack = queueChannelCallBack;
        this.requiredSegmentIds = new int[partitionIdAndSubpartitionIds.size()];
        Arrays.fill(requiredSegmentIds, -1);
        this.headerBuffer = ByteBuffer.wrap(new byte[HEADER_LENGTH]);
        headerBuffer.order(ByteOrder.nativeOrder());
        for (int index = 0; index < partitionIdAndSubpartitionIds.size(); ++index) {
            Tuple2<TieredStoragePartitionId, TieredStorageSubpartitionId> ids =
                    partitionIdAndSubpartitionIds.get(index);
            subpartitionIndexs
                    .computeIfAbsent(ids.f0, ignore -> new HashMap<>())
                    .put(ids.f1, index);
        }
    }

    @Override
    public void start() {
        remoteTierMonitor.start();
    }

    @Override
    public Optional<Buffer> getNextBuffer(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId2,
            int segmentId) {
        int subpartitionId = subpartitionIndexs.get(partitionId).get(subpartitionId2);
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
                queueChannelCallBack.accept(subpartitionId, false);
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
        ReadableByteChannel channel = Channels.newChannel(inputStream);

        //

        headerBuffer.clear();
        int bufferHeaderResult = channel.read(headerBuffer);
        if (bufferHeaderResult == -1) {
            throw new IOException("Empty header buffer is read from dfs.");
        }
        headerBuffer.rewind();
        BufferHeader header = parseBufferHeader(headerBuffer);
        ByteBuffer dataBuffer = ByteBuffer.allocateDirect(header.getLength());
        int dataBufferResult = channel.read(dataBuffer);
        if (dataBufferResult == -1) {
            throw new IOException("Empty data buffer is read from dfs.");
        }
        Buffer.DataType dataType = header.getDataType();
        return new NetworkBuffer(
                MemorySegmentFactory.wrapOffHeapMemory(dataBuffer),
                FreeingBufferRecycler.INSTANCE,
                dataType,
                header.isCompressed(),
                header.getLength());
    }
}
