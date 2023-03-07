package org.apache.flink.runtime.io.network.partition.tieredstore.downstream;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelBuilder;
import org.apache.flink.runtime.io.network.partition.consumer.TestInputChannel;
import org.apache.flink.runtime.io.network.partition.tieredstore.downstream.common.DownstreamTieredStoreMemoryManager;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collections;
import java.util.Optional;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.DATA_BUFFER;
import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.SEGMENT_EVENT;
import static org.apache.flink.runtime.io.network.partition.tieredstore.downstream.TieredStoreSingleInputGateTest.createTieredStoreSingleInputGate;
import static org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreUtils.createBaseSubpartitionPath;
import static org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TieredStoreUtils.writeSegmentFinishFile;
import static org.assertj.core.api.Assertions.assertThat;

/** The test for {@link SingleChannelLocalTierClient}. */
public class SingleChannelTierClientTest {

    private static final int SEGMENT_ID = 0;

    private static final int NUM_BUFFERS = 2;

    private static final int SUBPARTITION_INDEX = 0;

    private static final JobID JOB_ID = JobID.generate();

    private static final ResultPartitionID RESULT_PARTITION_ID = new ResultPartitionID();

    private String baseRemoteStoragePath;

    private TemporaryFolder temporaryFolder;

    @BeforeEach
    void before() throws IOException {
        temporaryFolder = TemporaryFolder.builder().build();
        temporaryFolder.create();
        baseRemoteStoragePath = temporaryFolder.getRoot().getPath();
    }

    @AfterEach
    void after() {
        temporaryFolder.delete();
    }

    @Test
    void testSingleChannelLocalTierClient() throws IOException, InterruptedException {
        SingleChannelLocalTierClient localTierClient = new SingleChannelLocalTierClient();
        final TieredStoreSingleInputGate inputGate =
                createTieredStoreSingleInputGate(2);
        InputChannel inputChannel1 =
                new InputChannelBuilder().setChannelIndex(0).buildRemoteRecoveredChannel(inputGate);
        verifyLocalTierClientResult(localTierClient, inputChannel1, false, null, SEGMENT_ID, false);
        InputChannel inputChannel2 =
                new InputChannelBuilder().setChannelIndex(1).buildLocalRecoveredChannel(inputGate);
        verifyLocalTierClientResult(localTierClient, inputChannel2, false, null, SEGMENT_ID, false);
        TestInputChannel inputChannel3 = new TestInputChannel(inputGate, SUBPARTITION_INDEX);
        verifyLocalTierClientResult(localTierClient, inputChannel3, false, null, SEGMENT_ID, false);
        inputChannel3.readBuffer();
        verifyLocalTierClientResult(localTierClient, inputChannel3, true, DATA_BUFFER, SEGMENT_ID, true);
        inputChannel3.readSegmentInfo(1);
        verifyLocalTierClientResult(localTierClient, inputChannel3, true, SEGMENT_EVENT, SEGMENT_ID, true);
        inputChannel3.readBuffer();
        verifyLocalTierClientResult(localTierClient, inputChannel3, true, DATA_BUFFER, 1, true);
        assertThat(inputChannel3.getRequiredSegmentId()).isEqualTo(1L);
    }

    @Test
    void testSingleChannelRemoteTierClient() throws Exception {
        final TieredStoreSingleInputGate inputGate =
                createTieredStoreSingleInputGate(2);
        SingleChannelRemoteTierClient remoteTierClient =
                new SingleChannelRemoteTierClient(
                        JOB_ID,
                        Collections.singletonList(RESULT_PARTITION_ID),
                        SUBPARTITION_INDEX,
                        new DownstreamTieredStoreMemoryManager(
                                (NetworkBufferPool) inputGate.getMemorySegmentProvider()),
                        baseRemoteStoragePath);
        InputChannel inputChannel1 =
                new InputChannelBuilder()
                        .setChannelIndex(SUBPARTITION_INDEX)
                        .buildRemoteRecoveredChannel(inputGate);
        verifyRemoteTierClientResult(remoteTierClient, inputChannel1, false, null, -1);
        InputChannel inputChannel2 =
                new InputChannelBuilder()
                        .setChannelIndex(SUBPARTITION_INDEX)
                        .buildLocalRecoveredChannel(inputGate);
        verifyRemoteTierClientResult(remoteTierClient, inputChannel2, false, null, -1);
        createShuffleFileOnRemoteStorage();
        TestInputChannel inputChannel3 = new TestInputChannel(inputGate, SUBPARTITION_INDEX);
        verifyRemoteTierClientResult(remoteTierClient, inputChannel3, true, DATA_BUFFER, SEGMENT_ID);
        verifyRemoteTierClientResult(remoteTierClient, inputChannel3, true, DATA_BUFFER, SEGMENT_ID);
    }

    private void verifyLocalTierClientResult(
            SingleChannelLocalTierClient client,
            InputChannel inputChannel,
            boolean isPresent,
            Buffer.DataType expectedDataType,
            int segmentId,
            boolean hasRegistered)
            throws IOException, InterruptedException {
        Optional<InputChannel.BufferAndAvailability> buffer =
                client.getNextBuffer(inputChannel, segmentId);
        if (buffer.isPresent()) {
            assertThat(buffer.get().buffer().getDataType()).isEqualTo(expectedDataType);
            assertThat(client.getLatestSegmentId()).isEqualTo(segmentId);
            assertThat(client.hasRegistered()).isEqualTo(hasRegistered);
            buffer.get().buffer().recycleBuffer();
        } else {
            assertThat(isPresent).isFalse();
            assertThat(client.getLatestSegmentId()).isEqualTo(segmentId);
            assertThat(client.hasRegistered()).isEqualTo(hasRegistered);
        }
    }

    private void verifyRemoteTierClientResult(
            SingleChannelRemoteTierClient client,
            InputChannel inputChannel,
            boolean isPresent,
            Buffer.DataType expectedDataType,
            int segmentId)
            throws IOException {
        Optional<InputChannel.BufferAndAvailability> buffer =
                client.getNextBuffer(inputChannel, segmentId);
        if (buffer.isPresent()) {
            assertThat(buffer.get().buffer().getDataType()).isEqualTo(expectedDataType);
            assertThat(client.getLatestSegmentId()).isEqualTo(segmentId);
            buffer.get().buffer().recycleBuffer();
        } else {
            assertThat(client.getLatestSegmentId()).isEqualTo(segmentId);
            assertThat(isPresent).isFalse();
        }
    }

    private void createShuffleFileOnRemoteStorage() throws Exception {
        String shuffleFilePath =
                createBaseSubpartitionPath(
                        JOB_ID,
                        RESULT_PARTITION_ID,
                        SUBPARTITION_INDEX,
                        baseRemoteStoragePath,
                        false);
        Path path = new Path(shuffleFilePath, "/seg-" + SEGMENT_ID);
        try (FSDataOutputStream outputStream =
                path.getFileSystem().create(path, FileSystem.WriteMode.OVERWRITE)) {
            for (int i = 0; i < NUM_BUFFERS; ++i) {
                writeSingleTestBuffer(outputStream);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to create shuffle file.");
        }
        writeSegmentFinishFile(shuffleFilePath, SEGMENT_ID);
    }

    private static void writeSingleTestBuffer(FSDataOutputStream outputStream) throws IOException {
        // 1. Build the data buffer
        byte[] stringData = new byte[10];
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
}
