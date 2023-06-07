package org.apache.flink.runtime.io.network.partition.hybrid.tiered.flink_31636;

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.NettyPayload;

/** Tests for {@link NettyPayload}. */
class BufferContextTest {
    //
    //@Test
    //void testGetBuffer() {
    //    Buffer buffer =
    //            new NetworkBuffer(
    //                    MemorySegmentFactory.allocateUnpooledSegment(0),
    //                    FreeingBufferRecycler.INSTANCE,
    //                    Buffer.DataType.DATA_BUFFER,
    //                    0);
    //    NettyPayload nettyPayload = new NettyPayload(buffer, 0, 0);
    //    Assertions.assertThat(nettyPayload.getBuffer()).isEqualTo(buffer);
    //}
    //
    //@Test
    //void testGetBufferNull() {
    //    NettyPayload nettyPayload = new NettyPayload(null, 0, 0);
    //    Assertions.assertThat(nettyPayload.getBuffer()).isNull();
    //}
}
