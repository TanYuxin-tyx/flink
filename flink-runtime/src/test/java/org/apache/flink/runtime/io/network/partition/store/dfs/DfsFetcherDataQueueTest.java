package org.apache.flink.runtime.io.network.partition.store.dfs;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.LocalBufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelBuilder;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannelTest;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateBuilder;
import org.apache.flink.runtime.io.network.partition.consumer.TestInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.tier.history.DfsFetcherDataQueue;
import org.apache.flink.runtime.io.network.partition.consumer.tier.history.FetcherDataQueueState;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.io.network.partition.store.common.TieredStoreUtils.createBaseSubpartitionPath;
import static org.assertj.core.api.Assertions.assertThat;

/** The test for {@link DfsFetcherDataQueue}. */
public class DfsFetcherDataQueueTest {

    public static final int NUM_BUFFERS = 100;

    public static final int BUFFERS_IN_SEGMENT = 10;

    public static final int MEMORY_SEGMENT_SIZE = 128;

    private static final String TEST_STRING = "abcdefghijklmn";

    private static final long SEGMENT_ID = 0;

    private DfsFetcherDataQueue dfsFetcherDataQueue;

    private NetworkBufferPool networkBufferPool;

    private LocalBufferPool localBufferPool;

    private final JobID jobID = JobID.generate();

    private final List<ResultPartitionID> resultPartitionIDS =
            new ArrayList<ResultPartitionID>() {
                {
                    add(new ResultPartitionID());
                }
            };

    private final int subpartitionIndex = 0;

    @TempDir public static java.nio.file.Path tempDataPath;

    private TemporaryFolder tmpFolder;

    @BeforeEach
    void before() throws Exception {
        tmpFolder = TemporaryFolder.builder().build();
        tmpFolder.create();
        resultPartitionIDS.add(new ResultPartitionID());
        networkBufferPool = new NetworkBufferPool(NUM_BUFFERS, MEMORY_SEGMENT_SIZE);
        localBufferPool = new LocalBufferPool(networkBufferPool, NUM_BUFFERS);
        dfsFetcherDataQueue = new DfsFetcherDataQueue();
        dfsFetcherDataQueue.setup(
                jobID,
                resultPartitionIDS,
                localBufferPool,
                subpartitionIndex,
                getTempStorePathDir());
    }

    @AfterEach
    void close() throws IOException {
        dfsFetcherDataQueue.close();
        localBufferPool.lazyDestroy();
        networkBufferPool.destroy();
        dfsFetcherDataQueue = null;
        localBufferPool = null;
        networkBufferPool = null;
    }

    @Test
    void testSetup() {
        assertThat(dfsFetcherDataQueue.getAvailableBuffers()).hasSize(1);
        assertThat(dfsFetcherDataQueue.getUsedBuffers()).hasSize(0);
        assertThat(dfsFetcherDataQueue.getCurSequenceNumber()).isEqualTo(-1);
        assertThat(dfsFetcherDataQueue.getCurrentChannel()).isEqualTo(Optional.empty());
        assertThat(dfsFetcherDataQueue.getState()).isNotEqualTo(FetcherDataQueueState.RUNNING);
    }

    @Test
    void testResolveSegmentInfo() throws Exception {
        int channelIndex = 0;
        SingleInputGate inputGate = new SingleInputGateBuilder().build();
        createSegmentInfoFile(SEGMENT_ID, channelIndex, subpartitionIndex);
        TestInputChannel inputChannel = new TestInputChannel(inputGate, channelIndex);
        dfsFetcherDataQueue.resolveSegmentInfo(SEGMENT_ID, inputChannel);
        assertThat(dfsFetcherDataQueue.getCurSequenceNumber()).isEqualTo(0);
        assertThat(dfsFetcherDataQueue.getCurrentDataPath())
                .isEqualTo(dfsFetcherDataQueue.getResolvedPath(SEGMENT_ID, inputChannel));
    }

    @Test
    void testResolveDfsBuffer() throws Exception {
        int channelIndex = 0;
        SingleInputGate inputGate = new SingleInputGateBuilder().build();
        TestInputChannel inputChannel = new TestInputChannel(inputGate, channelIndex);
        createSegmentInfoFile(SEGMENT_ID, channelIndex, subpartitionIndex);
        dfsFetcherDataQueue.resolveSegmentInfo(SEGMENT_ID, inputChannel);
        FSDataInputStream currentInputStream =
                dfsFetcherDataQueue
                        .getCurrentDataPath()
                        .getFileSystem()
                        .open(dfsFetcherDataQueue.getCurrentDataPath());
        for (int i = 0; i < BUFFERS_IN_SEGMENT; ++i) {
            assertThat(currentInputStream.available()).isGreaterThan(0);
            InputChannel.BufferAndAvailability bufferAndAvailability =
                    dfsFetcherDataQueue.resolveDfsBuffer(currentInputStream);
            assertThat(bufferAndAvailability.moreAvailable()).isTrue();
            assertThat(bufferAndAvailability.getSequenceNumber()).isEqualTo(i);
            NetworkBuffer buffer = (NetworkBuffer) bufferAndAvailability.buffer();
            byte[] data = new byte[buffer.getSize()];
            buffer.getBytes(0, data, 0, buffer.getSize());
            String result = new String(data, StandardCharsets.UTF_8);
            assertThat(result).isEqualTo(TEST_STRING);
        }
        assertThat(currentInputStream.available()).isEqualTo(0);
        currentInputStream.close();
    }

    @Test
    void testRun() throws Exception {
        int channelIndex = 0;
        createSegmentInfoFile(SEGMENT_ID, channelIndex, subpartitionIndex);
        setSegmentInfoToDfsDataFetcher();
        while (dfsFetcherDataQueue.getUsedBuffers().size() < BUFFERS_IN_SEGMENT) {
            TimeUnit.MILLISECONDS.sleep(10);
        }
        ArrayDeque<InputChannel.BufferAndAvailability> usedBuffers =
                dfsFetcherDataQueue.getUsedBuffers();
        assertThat(usedBuffers).hasSize(BUFFERS_IN_SEGMENT);
    }

    @Test
    void testGetNextBufferInBlockingMode() throws Exception {
        int channelIndex = 0;
        createSegmentInfoFile(SEGMENT_ID, channelIndex, subpartitionIndex);
        setSegmentInfoToDfsDataFetcher();
        for (int i = 0; i < BUFFERS_IN_SEGMENT; ++i) {
            Optional<InputChannel.BufferAndAvailability> nextBuffer =
                    dfsFetcherDataQueue.getNextBuffer(true);
            assertThat(nextBuffer.isPresent()).isTrue();
            assertThat(nextBuffer.get().getSequenceNumber()).isEqualTo(i);
            assertThat(nextBuffer.get().buffersInBacklog()).isEqualTo(0);
            assertThat(nextBuffer.get().moreAvailable()).isTrue();
            assertThat(nextBuffer.get().morePriorityEvents()).isFalse();
            assertThat(nextBuffer.get().hasPriority()).isFalse();
            NetworkBuffer buffer = (NetworkBuffer) nextBuffer.get().buffer();
            byte[] data = new byte[buffer.getSize()];
            buffer.getBytes(0, data, 0, buffer.getSize());
            String result = new String(data, StandardCharsets.UTF_8);
            assertThat(result).isEqualTo(TEST_STRING);
        }
        assertThat(dfsFetcherDataQueue.getUsedBuffers()).hasSize(0);
        assertThat(dfsFetcherDataQueue.getAvailableBuffers()).hasSize(0);
    }

    @Test
    void testGetNextBufferInNonBlockingMode() throws Exception {
        int channelIndex = 0;
        createSegmentInfoFile(SEGMENT_ID, channelIndex, subpartitionIndex);
        setSegmentInfoToDfsDataFetcher();
        for (int i = 0; i < BUFFERS_IN_SEGMENT; ++i) {
            Optional<InputChannel.BufferAndAvailability> nextBuffer = Optional.empty();
            while (!nextBuffer.isPresent()) {
                nextBuffer = dfsFetcherDataQueue.getNextBuffer(false);
            }
            assertThat(nextBuffer.get().getSequenceNumber()).isEqualTo(i);
            assertThat(nextBuffer.get().buffersInBacklog()).isEqualTo(0);
            assertThat(nextBuffer.get().moreAvailable()).isTrue();
            assertThat(nextBuffer.get().morePriorityEvents()).isFalse();
            assertThat(nextBuffer.get().hasPriority()).isFalse();
            NetworkBuffer buffer = (NetworkBuffer) nextBuffer.get().buffer();
            byte[] data = new byte[buffer.getSize()];
            buffer.getBytes(0, data, 0, buffer.getSize());
            String result = new String(data, StandardCharsets.UTF_8);
            assertThat(result).isEqualTo(TEST_STRING);
        }
        assertThat(dfsFetcherDataQueue.getUsedBuffers()).hasSize(0);
        assertThat(dfsFetcherDataQueue.getAvailableBuffers()).hasSize(0);
    }

    private void setSegmentInfoToDfsDataFetcher() throws Exception {
        SingleInputGate inputGate =
                new SingleInputGateBuilder().setBufferPoolFactory(localBufferPool).build();
        int inputChannelIndex = 0;
        RemoteInputChannel inputChannel =
                InputChannelBuilder.newBuilder()
                        .setConnectionManager(
                                new RemoteInputChannelTest.TestVerifyConnectionManager(
                                        new RemoteInputChannelTest
                                                .TestVerifyPartitionRequestClient()))
                        .setChannelIndex(inputChannelIndex)
                        .buildRemoteChannel(inputGate);
        dfsFetcherDataQueue.setSegmentInfo(inputChannel, SEGMENT_ID);
    }

    private void createSegmentInfoFile(long segmentId, int channelIndex, int subpartitionIndex)
            throws Exception {
        String baseSubpartitionPath =
                createBaseSubpartitionPath(
                        jobID,
                        resultPartitionIDS.get(channelIndex),
                        subpartitionIndex,
                        getTempStorePathDir(),
                        false);
        Path path = new Path(baseSubpartitionPath, "/seg-" + segmentId);
        writeSegmentInfoToFile(path);
    }

    public static void writeSegmentInfoToFile(Path path) throws Exception {
        FileSystem fileSystem = FileSystem.getLocalFileSystem();
        FSDataOutputStream outputStream = fileSystem.create(path, FileSystem.WriteMode.OVERWRITE);
        for (int i = 0; i < BUFFERS_IN_SEGMENT; ++i) {
            writeSingleBuffer(outputStream);
        }
        outputStream.close();
    }

    public static void deleteTempSegmentFile(Path path) throws Exception {
        FileSystem fileSystem = FileSystem.getLocalFileSystem();
        fileSystem.delete(path, true);
    }

    private static void writeSingleBuffer(FSDataOutputStream outputStream) throws IOException {
        // 1. Build the data buffer
        byte[] stringData = TEST_STRING.getBytes(StandardCharsets.UTF_8);
        NetworkBuffer dataBuffer =
                new NetworkBuffer(
                        MemorySegmentFactory.allocateUnpooledSegment(stringData.length),
                        BufferRecycler.DummyBufferRecycler.INSTANCE);
        dataBuffer.writeBytes(stringData);
        // 2. Build the data header buffer
        ByteBuffer headerBuffer = ByteBuffer.wrap(new byte[8]);
        headerBuffer.order(ByteOrder.nativeOrder());
        BufferReaderWriterUtil.setByteChannelBufferHeader(dataBuffer, headerBuffer);
        outputStream.write(headerBuffer.array());
        outputStream.write(dataBuffer.getMemorySegment().getArray());
        dataBuffer.recycleBuffer();
    }

    private String getTempStorePathDir() {
        return tmpFolder.getRoot().getPath();
    }
}
