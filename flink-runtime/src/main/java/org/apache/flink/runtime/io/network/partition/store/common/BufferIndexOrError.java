package org.apache.flink.runtime.io.network.partition.store.common;

import org.apache.flink.runtime.io.network.buffer.Buffer;

import javax.annotation.Nullable;

import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Indicates a buffer with index or an error. */
public class BufferIndexOrError {
    @Nullable private final Buffer buffer;
    private final int index;
    @Nullable private final Throwable throwable;

    private BufferIndexOrError(@Nullable Buffer buffer, int index, @Nullable Throwable throwable) {
        this.buffer = buffer;
        this.index = index;
        this.throwable = throwable;
    }

    public Buffer.DataType getDataType() {
        return buffer == null ? Buffer.DataType.NONE : buffer.getDataType();
    }

    public static BufferIndexOrError newError(Throwable throwable) {
        return new BufferIndexOrError(null, -1, checkNotNull(throwable));
    }

    public static BufferIndexOrError newBuffer(Buffer buffer, int index) {
        return new BufferIndexOrError(checkNotNull(buffer), index, null);
    }

    public Optional<Buffer> getBuffer() {
        return Optional.ofNullable(buffer);
    }

    public Optional<Throwable> getThrowable() {
        return Optional.ofNullable(throwable);
    }

    public int getIndex() {
        checkNotNull(buffer, "Is error, cannot get index.");
        return index;
    }
}
