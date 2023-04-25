package org.apache.flink.runtime.io.network.partition.hybrid.tiered.flink_31636;

import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.BufferContext;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.BufferIndexAndSubpartitionId;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for {@link BufferContext}. */
class BufferContextTest {

    @Test
    void testGetBuffer() {
        Buffer buffer =
                new NetworkBuffer(
                        MemorySegmentFactory.allocateUnpooledSegment(0),
                        FreeingBufferRecycler.INSTANCE,
                        Buffer.DataType.DATA_BUFFER,
                        0);
        BufferContext bufferContext = new BufferContext(buffer, 0, 0);
        Assertions.assertThat(bufferContext.getBuffer()).isEqualTo(buffer);
    }

    @Test
    void testGetBufferIndexAndChannel() {
        BufferIndexAndSubpartitionId bufferIndexAndChannel = new BufferIndexAndSubpartitionId(0, 0);
        BufferContext bufferContext = new BufferContext(null, bufferIndexAndChannel, null);
        Assertions.assertThat(bufferContext.getBufferIndexAndChannel())
                .isEqualTo(bufferIndexAndChannel);
    }

    @Test
    void testGetThrowable() {
        Throwable throwable = new Throwable();
        BufferContext bufferContext = new BufferContext(null, null, throwable);
        Assertions.assertThat(bufferContext.getThrowable()).isEqualTo(throwable);
    }

    @Test
    void testGetBufferNull() {
        BufferContext bufferContext = new BufferContext(null, 0, 0);
        Assertions.assertThat(bufferContext.getBuffer()).isNull();
    }

    @Test
    void testGetBufferIndexAndChannelNull() {
        BufferContext bufferContext = new BufferContext(null, null, null);
        Assertions.assertThat(bufferContext.getBufferIndexAndChannel()).isNull();
    }

    @Test
    void testGetThrowableNull() {
        BufferContext bufferContext = new BufferContext(null, null, null);
        Assertions.assertThat(bufferContext.getThrowable()).isNull();
    }
}
