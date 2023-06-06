package org.apache.flink.runtime.io.network.partition.hybrid.tiered.flink_31636;

import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.NettyPayload;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for {@link NettyPayload}. */
class BufferContextTest {

    @Test
    void testGetBuffer() {
        Buffer buffer =
                new NetworkBuffer(
                        MemorySegmentFactory.allocateUnpooledSegment(0),
                        FreeingBufferRecycler.INSTANCE,
                        Buffer.DataType.DATA_BUFFER,
                        0);
        NettyPayload nettyPayload = new NettyPayload(buffer, 0, 0);
        Assertions.assertThat(nettyPayload.getBuffer()).isEqualTo(buffer);
    }

    @Test
    void testGetBufferNull() {
        NettyPayload nettyPayload = new NettyPayload(null, 0, 0);
        Assertions.assertThat(nettyPayload.getBuffer()).isNull();
    }
}
