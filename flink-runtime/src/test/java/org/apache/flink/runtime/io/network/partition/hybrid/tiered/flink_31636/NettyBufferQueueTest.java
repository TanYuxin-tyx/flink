package org.apache.flink.runtime.io.network.partition.hybrid.tiered.flink_31636;

import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.service.NettyBufferQueue;

import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link NettyBufferQueue}. */
class NettyBufferQueueTest {

    @Test
    void testGetNextBuffer() throws Throwable {
        int index = 1;
        int backlog = 10;
        TestingNettyBufferQueue testingNettyBufferQueue =
                TestingNettyBufferQueue.builder()
                        .setConsumeBufferFunction(
                                (bufferIndex) ->
                                        Optional.of(
                                                createBufferAndBacklog(
                                                        backlog,
                                                        Buffer.DataType.DATA_BUFFER,
                                                        bufferIndex)))
                        .build();
        Optional<ResultSubpartition.BufferAndBacklog> nextBuffer =
                testingNettyBufferQueue.getNextBuffer(index);
        assertThat(nextBuffer).isPresent();
        assertThat(nextBuffer.get().getSequenceNumber()).isEqualTo(index);
        assertThat(nextBuffer.get().buffersInBacklog()).isEqualTo(backlog);
    }

    @Test
    void testGetBacklog() {
        int backlog = 10;
        TestingNettyBufferQueue testingNettyBufferQueue =
                TestingNettyBufferQueue.builder().setGetBacklogSupplier(() -> backlog).build();
        assertThat(testingNettyBufferQueue.getBacklog()).isEqualTo(backlog);
    }

    @Test
    void testRelease() {
        AtomicBoolean isReleased = new AtomicBoolean(false);
        TestingNettyBufferQueue testingNettyBufferQueue =
                TestingNettyBufferQueue.builder()
                        .setReleaseDataViewRunnable(() -> isReleased.set(true))
                        .build();
        testingNettyBufferQueue.release();
        assertThat(isReleased.get()).isTrue();
    }

    public static ResultSubpartition.BufferAndBacklog createBufferAndBacklog(
            int buffersInBacklog, Buffer.DataType nextDataType, int sequenceNumber) {
        int bufferSize = 8;
        Buffer buffer =
                new NetworkBuffer(
                        MemorySegmentFactory.allocateUnpooledSegment(bufferSize),
                        FreeingBufferRecycler.INSTANCE,
                        Buffer.DataType.DATA_BUFFER,
                        bufferSize);
        return new ResultSubpartition.BufferAndBacklog(
                buffer, buffersInBacklog, nextDataType, sequenceNumber);
    }
}
