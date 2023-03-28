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

/** The test for {@link LocalTierClient}. */
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
        LocalTierClient localTierClient = new LocalTierClient();
        final TieredStoreSingleInputGate inputGate = createTieredStoreSingleInputGate(2);
        InputChannel inputChannel1 =
                new InputChannelBuilder().setChannelIndex(0).buildRemoteRecoveredChannel(inputGate);
        verifyLocalTierClientResult(localTierClient, inputChannel1, false, null, SEGMENT_ID, false);
        InputChannel inputChannel2 =
                new InputChannelBuilder().setChannelIndex(1).buildLocalRecoveredChannel(inputGate);
        verifyLocalTierClientResult(localTierClient, inputChannel2, false, null, SEGMENT_ID, false);
        TestInputChannel inputChannel3 = new TestInputChannel(inputGate, SUBPARTITION_INDEX);
        verifyLocalTierClientResult(localTierClient, inputChannel3, false, null, SEGMENT_ID, false);
        inputChannel3.readBuffer();
        verifyLocalTierClientResult(
                localTierClient, inputChannel3, true, DATA_BUFFER, SEGMENT_ID, true);
        inputChannel3.readSegmentInfo(1);
        verifyLocalTierClientResult(
                localTierClient, inputChannel3, true, SEGMENT_EVENT, SEGMENT_ID, true);
        inputChannel3.readBuffer();
        verifyLocalTierClientResult(localTierClient, inputChannel3, true, DATA_BUFFER, 1, true);
        assertThat(inputChannel3.getRequiredSegmentId()).isEqualTo(1L);
    }

    @Test
    void testSingleChannelRemoteTierClient() throws Exception {
        final TieredStoreSingleInputGate inputGate = createTieredStoreSingleInputGate(2);
        RemoteTierMonitor remoteTierMonitor =
                new RemoteTierMonitor(
                        JOB_ID,
                        Collections.singletonList(RESULT_PARTITION_ID),
                        baseRemoteStoragePath,
                        Collections.singletonList(SUBPARTITION_INDEX));
        RemoteTierClient remoteTierClient =
                new RemoteTierClient(
                        new DownstreamTieredStoreMemoryManager(
                                (NetworkBufferPool) inputGate.getMemorySegmentProvider()),
                        remoteTierMonitor);
        InputChannel targetInputChannel =
                new InputChannelBuilder()
                        .setChannelIndex(SUBPARTITION_INDEX)
                        .buildRemoteRecoveredChannel(inputGate);
        InputChannel[] inputChannels = new InputChannel[1];
        inputChannels[0] = targetInputChannel;
        remoteTierMonitor.setup(inputChannels, channel -> {});
        verifyRemoteTierClientResult(remoteTierClient, targetInputChannel, false, null, -1);
        createShuffleFileOnRemoteStorage();
        verifyRemoteTierClientResult(remoteTierClient, targetInputChannel, true, DATA_BUFFER, SEGMENT_ID);
    }

    private void verifyLocalTierClientResult(
            LocalTierClient client,
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
            RemoteTierClient client,
            InputChannel inputChannel,
            boolean isPresent,
            Buffer.DataType expectedDataType,
            int segmentId)
            throws IOException {

        if (isPresent) {
            while (true) {
                Optional<InputChannel.BufferAndAvailability> buffer =
                        client.getNextBuffer(inputChannel, segmentId);
                if (buffer.isPresent()) {
                    assertThat(buffer.get().buffer().getDataType()).isEqualTo(expectedDataType);
                    assertThat(client.getLatestSegmentId()).isEqualTo(segmentId);
                    buffer.get().buffer().recycleBuffer();
                    break;
                }
            }

        } else {
            Optional<InputChannel.BufferAndAvailability> buffer =
                    client.getNextBuffer(inputChannel, segmentId);
            assertThat(client.getLatestSegmentId()).isEqualTo(segmentId);
            assertThat(buffer.isPresent()).isFalse();
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
        NetworkBuffer dataBuffer =
                new NetworkBuffer(
                        MemorySegmentFactory.allocateUnpooledSegment(1),
                        BufferRecycler.DummyBufferRecycler.INSTANCE);
        ByteBuffer headerBuffer = ByteBuffer.wrap(new byte[8]);
        headerBuffer.order(ByteOrder.nativeOrder());
        BufferReaderWriterUtil.setByteChannelBufferHeader(dataBuffer, headerBuffer);
        outputStream.write(headerBuffer.array());
        outputStream.write(dataBuffer.getMemorySegment().getArray());
        dataBuffer.recycleBuffer();
    }
}
