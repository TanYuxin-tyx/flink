package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.remote;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.api.EndOfSegmentEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.BufferHeader;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager1;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierConsumerAgent;
import org.apache.flink.util.ExceptionUtils;

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
public class RemoteTierConsumerAgent implements TierConsumerAgent {

    private final SubpartitionConsumerAgent[] consumerAgents;

    private final RemoteTierMonitor remoteTierMonitor;

    public RemoteTierConsumerAgent(
            int numInputChannels,
            TieredStorageMemoryManager1 memoryManager1,
            RemoteTierMonitor remoteTierMonitor,
            NettyService consumerNettyService) {
        this.remoteTierMonitor = remoteTierMonitor;
        consumerAgents = new SubpartitionConsumerAgent[numInputChannels];
        for (int index = 0; index < numInputChannels; ++index) {
            consumerAgents[index] =
                    new SubpartitionConsumerAgent(
                            memoryManager1, remoteTierMonitor, consumerNettyService);
        }
    }

    @Override
    public void start() {
        remoteTierMonitor.start();
    }

    @Override
    public Optional<Buffer> getNextBuffer(int subpartitionId, int segmentId) {
        Optional<Buffer> buffer = Optional.empty();
        try {
            buffer = consumerAgents[subpartitionId].getNextBuffer(subpartitionId, segmentId);
        } catch (IOException e) {
            ExceptionUtils.rethrow(e, "Failed to get next buffer.");
        }
        return buffer;
    }

    @Override
    public void close() throws IOException {
        for (SubpartitionConsumerAgent subpartitionConsumerAgent : consumerAgents) {
            subpartitionConsumerAgent.close();
        }
        remoteTierMonitor.close();
    }

    private class SubpartitionConsumerAgent {
        private final TieredStorageMemoryManager1 memoryManager1;

        private final ByteBuffer headerBuffer;

        private final RemoteTierMonitor remoteTierMonitor;

        private final NettyService consumerNettyService;

        private FSDataInputStream currentInputStream;

        private int latestSegmentId = -1;

        private SubpartitionConsumerAgent(
                TieredStorageMemoryManager1 memoryManager1,
                RemoteTierMonitor remoteTierMonitor,
                NettyService consumerNettyService) {
            this.headerBuffer = ByteBuffer.wrap(new byte[HEADER_LENGTH]);
            headerBuffer.order(ByteOrder.nativeOrder());
            this.memoryManager1 = memoryManager1;
            this.remoteTierMonitor = remoteTierMonitor;
            this.consumerNettyService = consumerNettyService;
        }

        public Optional<Buffer> getNextBuffer(int subpartitionId, int segmentId)
                throws IOException {
            if (segmentId != latestSegmentId) {
                remoteTierMonitor.requireSegmentId(subpartitionId, segmentId);
                latestSegmentId = segmentId;
            }
            if (!remoteTierMonitor.isExist(subpartitionId, segmentId)) {
                return Optional.empty();
            }
            currentInputStream = remoteTierMonitor.getInputStream(subpartitionId, segmentId);
            if (currentInputStream.available() == 0) {
                currentInputStream.close();
                return Optional.of(buildEndOfSegmentBuffer(latestSegmentId + 1));
            } else {
                consumerNettyService.notifyResultSubpartitionAvailable(subpartitionId, false);
                return Optional.of(getDfsBuffer(currentInputStream));
            }
        }

        public void close() throws IOException {
            if (currentInputStream != null) {
                currentInputStream.close();
            }
        }

        // ------------------------------------
        //           Internal Method
        // ------------------------------------

        private Buffer getDfsBuffer(FSDataInputStream inputStream) throws IOException {
            BufferBuilder builder = memoryManager1.requestBufferBlocking(this);
            BufferConsumer bufferConsumer = builder.createBufferConsumer();
            Buffer buffer = bufferConsumer.build();
            return checkNotNull(
                    readFromInputStream(
                            buffer.getMemorySegment(), inputStream, buffer.getRecycler()));
        }

        private Buffer readFromInputStream(
                MemorySegment memorySegment,
                FSDataInputStream inputStream,
                BufferRecycler bufferRecycler)
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
                    memorySegment,
                    bufferRecycler,
                    dataType,
                    header.isCompressed(),
                    header.getLength());
        }

        private Buffer buildEndOfSegmentBuffer(int segmentId) throws IOException {
            MemorySegment data =
                    MemorySegmentFactory.wrap(
                            EventSerializer.toSerializedEvent(EndOfSegmentEvent.INSTANCE).array());
            return new NetworkBuffer(
                    data,
                    FreeingBufferRecycler.INSTANCE,
                    Buffer.DataType.ADD_SEGMENT_ID_EVENT,
                    data.size());
        }

        @VisibleForTesting
        public int getLatestSegmentId() {
            return latestSegmentId;
        }
    }
}
