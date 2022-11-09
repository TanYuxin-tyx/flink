package org.apache.flink.runtime.io.network.partition.store.common;

import org.apache.flink.runtime.io.network.buffer.Buffer;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.apache.flink.runtime.io.network.partition.store.TieredStoreTestUtils.createBuffer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link BufferIndexOrError}. */
class BufferIndexOrErrorTest {

    private static final int BUFFER_SIZE = 16;

    private static final int BUFFER_INDEX = 0;

    public static final Buffer.DataType DEFAULT_DATATYPE = Buffer.DataType.DATA_BUFFER;

    @Test
    void testGetCorrectIndexAndErrorAndBuffer() {
        Buffer buffer = createBuffer(BUFFER_SIZE, false);
        int index = BUFFER_INDEX;
        Throwable error = new Throwable();
        BufferIndexOrError bufferIndexOrError1 = BufferIndexOrError.newBuffer(buffer, index);

        assertThat(bufferIndexOrError1.getIndex()).isEqualTo(index);
        assertThat(bufferIndexOrError1.getBuffer()).isEqualTo(Optional.of(buffer));
        assertThat(bufferIndexOrError1.getThrowable().isPresent()).isEqualTo(false);
        assertThat(bufferIndexOrError1.getDataType()).isEqualTo(DEFAULT_DATATYPE);

        BufferIndexOrError bufferIndexOrError2 = BufferIndexOrError.newError(error);

        assertThatThrownBy(bufferIndexOrError2::getIndex).isInstanceOf(NullPointerException.class);
        assertThat(bufferIndexOrError2.getBuffer()).isEqualTo(Optional.empty());
        assertThat(bufferIndexOrError2.getThrowable()).isEqualTo(Optional.of(error));
        assertThat(bufferIndexOrError2.getDataType()).isEqualTo(Buffer.DataType.NONE);
    }
}
