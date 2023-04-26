package org.apache.flink.runtime.io.network.partition.hybrid.tiered.flink_31638;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.LocalBufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelBuilder;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.TestInputChannel;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TierConfSpec;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TierType;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManagerImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.IndexedTierConfSpec;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.local.LocalTierConsumerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.remote.RemoteTierConsumerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.remote.RemoteTierMonitor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Optional;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.ADD_SEGMENT_ID_EVENT;
import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.DATA_BUFFER;
import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.createSingleInputGate;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageUtils.createBaseSubpartitionPath;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageUtils.writeSegmentFinishFile;
import static org.assertj.core.api.Assertions.assertThat;

/** The test for {@link LocalTierConsumerAgent}. */
public class ConsumerAgentTest {

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
    void testLocalTierConsumerAgent() throws IOException, InterruptedException {
        LocalTierConsumerAgent localConsumerAgent = new LocalTierConsumerAgent();
        final SingleInputGate inputGate = createSingleInputGate(2);
        InputChannel inputChannel1 =
                new InputChannelBuilder().setChannelIndex(0).buildRemoteRecoveredChannel(inputGate);
        verifyLocalTierConsumerAgentResult(
                localConsumerAgent, inputChannel1, false, null, SEGMENT_ID);
        InputChannel inputChannel2 =
                new InputChannelBuilder().setChannelIndex(1).buildLocalRecoveredChannel(inputGate);
        verifyLocalTierConsumerAgentResult(
                localConsumerAgent, inputChannel2, false, null, SEGMENT_ID);
        TestInputChannel inputChannel3 = new TestInputChannel(inputGate, SUBPARTITION_INDEX);
        verifyLocalTierConsumerAgentResult(
                localConsumerAgent, inputChannel3, false, null, SEGMENT_ID);
        inputChannel3.readBuffer();
        verifyLocalTierConsumerAgentResult(
                localConsumerAgent, inputChannel3, true, DATA_BUFFER, SEGMENT_ID);
        inputChannel3.readSegmentInfo();
        verifyLocalTierConsumerAgentResult(
                localConsumerAgent, inputChannel3, true, ADD_SEGMENT_ID_EVENT, SEGMENT_ID);
        inputChannel3.readBuffer();
        verifyLocalTierConsumerAgentResult(localConsumerAgent, inputChannel3, true, DATA_BUFFER, 1);
        assertThat(inputChannel3.getRequiredSegmentId()).isEqualTo(1L);
    }

    @Test
    void testRemoteTierConsumerAgent() throws Exception {
        RemoteTierMonitor remoteTierMonitor =
                new RemoteTierMonitor(
                        JOB_ID,
                        Collections.singletonList(RESULT_PARTITION_ID),
                        baseRemoteStoragePath,
                        Collections.singletonList(SUBPARTITION_INDEX),
                        1,
                        false,
                        i -> {});
        TieredStorageMemoryManager memoryManager =
                new TieredStorageMemoryManagerImpl(
                        new ArrayList<IndexedTierConfSpec>() {
                            {
                                add(
                                        new IndexedTierConfSpec(
                                                0, new TierConfSpec(TierType.IN_REMOTE, 1, true)));
                            }
                        });
        memoryManager.setup(new LocalBufferPool(new NetworkBufferPool(1, 1), 1));
        RemoteTierConsumerAgent remoteConsumerAgent =
                new RemoteTierConsumerAgent(memoryManager, remoteTierMonitor);
        final SingleInputGate inputGate = createSingleInputGate(2, new NetworkBufferPool(1, 1));
        InputChannel targetInputChannel =
                new InputChannelBuilder()
                        .setChannelIndex(SUBPARTITION_INDEX)
                        .buildRemoteChannel(inputGate);
        verifyRemoteTierConsumerAgentResult(
                remoteConsumerAgent, targetInputChannel, false, null, -1);
        createShuffleFileOnRemoteStorage();
        verifyRemoteTierConsumerAgentResult(
                remoteConsumerAgent, targetInputChannel, true, DATA_BUFFER, SEGMENT_ID);
    }

    private void verifyLocalTierConsumerAgentResult(
            LocalTierConsumerAgent client,
            InputChannel inputChannel,
            boolean isPresent,
            Buffer.DataType expectedDataType,
            int segmentId)
            throws IOException, InterruptedException {
        Optional<InputChannel.BufferAndAvailability> buffer =
                client.getNextBuffer(inputChannel, segmentId);
        if (buffer.isPresent()) {
            assertThat(buffer.get().buffer().getDataType()).isEqualTo(expectedDataType);
            assertThat(client.getLatestSegmentId()).isEqualTo(segmentId);
            buffer.get().buffer().recycleBuffer();
        } else {
            assertThat(isPresent).isFalse();
            assertThat(client.getLatestSegmentId()).isEqualTo(segmentId);
        }
    }

    private void verifyRemoteTierConsumerAgentResult(
            RemoteTierConsumerAgent remoteTierConsumerAgent,
            InputChannel inputChannel,
            boolean isPresent,
            Buffer.DataType expectedDataType,
            int segmentId)
            throws IOException {
        if (isPresent) {
            while (true) {
                Optional<InputChannel.BufferAndAvailability> buffer =
                        remoteTierConsumerAgent.getNextBuffer(inputChannel, segmentId);
                if (buffer.isPresent()) {
                    assertThat(buffer.get().buffer().getDataType()).isEqualTo(expectedDataType);
                    assertThat(remoteTierConsumerAgent.getLatestSegmentId()).isEqualTo(segmentId);
                    buffer.get().buffer().recycleBuffer();
                    break;
                }
            }

        } else {
            Optional<InputChannel.BufferAndAvailability> buffer =
                    remoteTierConsumerAgent.getNextBuffer(inputChannel, segmentId);
            assertThat(remoteTierConsumerAgent.getLatestSegmentId()).isEqualTo(segmentId);
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
