/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;

import org.apache.commons.lang3.tuple.Pair;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The {@link OldSortBuffer} is used to accumulate the records into {@link Buffer}s. The {@link
 * OldSortBuffer} allows for writing data in arbitrary subpartition orders but supports reading of
 * data in the order grouped by subpartitions. Note that the {@link OldSortBuffer} only supports
 * reading after the write process finished, and can not support reading while writing.
 */
public class OldSortBuffer {

    /**
     * Size of an index entry: 4 bytes for record length, 4 bytes for data type and 8 bytes for
     * pointer to next entry.
     */
    private static final int INDEX_ENTRY_SIZE = 4 + 4 + 8;

    /** A list of {@link MemorySegment}s used to store data in memory. */
    private final LinkedList<MemorySegment> freeSegments;

    /** A segment list as a joint buffer which stores all records and index entries. */
    private final ArrayList<MemorySegment> dataSegments;

    /** {@link BufferRecycler} used to recycle {@link #freeSegments}. */
    private final BufferRecycler bufferRecycler;

    /** Addresses of the first record's index entry for each subpartition. */
    private final long[] subpartitionFirstBufferIndexEntries;

    /** Addresses of the last record's index entry for each subpartition. */
    private final long[] subpartitionLastBufferIndexEntries;

    /** Size of buffers requested from buffer pool. All buffers must be of the same size. */
    private final int bufferSizeBytes;

    /** Number of guaranteed buffers can be allocated from the buffer pool for data sort. */
    private final int numBuffersForSort;

    // ------------------------------------------------------------------------
    // The statistics and states
    // ------------------------------------------------------------------------

    /** Total number of bytes already appended to this sort buffer. */
    private long numTotalBytes;

    /** Total number of bytes already read from this sort buffer. */
    private long numTotalBytesRead;

    /** Whether this sort buffer is finished. One can only read a finished sort buffer. */
    private boolean isFinished;

    /** Whether this sort buffer is released. A released sort buffer can not be used. */
    private boolean isReleased;

    // ------------------------------------------------------------------------
    // For writing
    // ------------------------------------------------------------------------

    /** Array index in the segment list of the current available buffer for writing. */
    private int writeBufferIndex;

    /** Next position in the current available buffer for writing. */
    private int writeOffsetInCurrentBuffer;

    // ------------------------------------------------------------------------
    // For reading
    // ------------------------------------------------------------------------

    /** Index entry address of the current record or event to be read. */
    private long readBufferIndexEntry;

    /**
     * Record the bytes remaining after the last read, which must be initialized before reading a
     * new record.
     */
    private int recordRemainingBytesToRead;

    /** The subpartition that is reading data from. */
    private int readingSubpartitionId = -1;

    OldSortBuffer(
            LinkedList<MemorySegment> freeSegments,
            BufferRecycler bufferRecycler,
            int numSubpartitions,
            int bufferSizeBytes,
            int numBuffersForSort) {
        checkArgument(bufferSizeBytes > INDEX_ENTRY_SIZE, "Buffer size is too small.");
        checkArgument(numBuffersForSort > 0, "No guaranteed buffers for sort.");
        checkState(numBuffersForSort <= freeSegments.size(), "Wrong number of free segments.");

        this.freeSegments = checkNotNull(freeSegments);
        this.bufferRecycler = checkNotNull(bufferRecycler);
        this.bufferSizeBytes = bufferSizeBytes;
        this.numBuffersForSort = numBuffersForSort;
        this.dataSegments = new ArrayList<>();
        this.subpartitionFirstBufferIndexEntries = new long[numSubpartitions];
        this.subpartitionLastBufferIndexEntries = new long[numSubpartitions];

        Arrays.fill(subpartitionFirstBufferIndexEntries, -1L);
        Arrays.fill(subpartitionLastBufferIndexEntries, -1L);
    }

    // ------------------------------------------------------------------------
    //  Called by SortBufferAccumulator
    // ------------------------------------------------------------------------

    /**
     * Note that no partial records will be written to this {@link OldSortBuffer}, which means that
     * either all data of target record will be written or nothing will be written.
     *
     * @param record the record to be written
     * @param subpartitionId the subpartition id
     * @param dataType the data type of the record
     * @return true if writing the record successfully, otherwise return false.
     */
    boolean writeRecord(ByteBuffer record, int subpartitionId, Buffer.DataType dataType) {
        checkArgument(record.hasRemaining(), "Cannot writeRecord empty data.");
        checkState(!isFinished, "Sort buffer is already finished.");
        checkState(!isReleased, "Sort buffer is already released.");

        int totalBytes = record.remaining();

        // Return false directly if it can not allocate enough buffers for the given record
        if (!allocateBuffersForRecord(totalBytes)) {
            return false;
        }

        // WriteRecord the index entry and record or event data
        writeIndex(subpartitionId, totalBytes, dataType);
        writeRecord(record);

        numTotalBytes += totalBytes;
        return true;
    }

    /**
     * Read the sorted buffers.
     *
     * @param readMemorySegment the buffer to store the data to be read
     * @return null iff all the data has been read from the {@link OldSortBuffer}, or return the
     *     pair of subpartition id and the read buffer
     */
    Pair<Integer, Buffer> readBuffer(MemorySegment readMemorySegment) {
        checkState(isFinished, "Sort buffer is not ready to be read.");
        checkState(!isReleased, "Sort buffer is already released.");

        if (!hasRemaining()) {
            return null;
        }

        int numBytesRead = 0;
        Buffer.DataType bufferDataType = Buffer.DataType.DATA_BUFFER;
        int currentReadingSubpartitionId = readingSubpartitionId;

        do {
            // Get the buffer index and offset from the index entry
            int toReadBufferIndex = getBufferIdFromBufferIndexEntry(readBufferIndexEntry);
            int toReadOffsetInBuffer = getOffsetInBufferFromBufferIndexEntry(readBufferIndexEntry);

            // Get the lengthAndDataType buffer according the buffer index
            MemorySegment toReadBuffer = dataSegments.get(toReadBufferIndex);

            // From the lengthAndDataType buffer, read and get the length and the data type
            long lengthAndDataType = toReadBuffer.getLong(toReadOffsetInBuffer);
            int recordLength = getRecordLength(lengthAndDataType);
            Buffer.DataType dataType = getBufferDataType(lengthAndDataType);

            // If the buffer is an event and some data has been read, return it directly to ensure
            // that the event will occupy one buffer independently
            if (dataType.isEvent() && numBytesRead > 0) {
                break;
            }
            bufferDataType = dataType;

            // Get the next index entry address and move the read position forward
            long nextReadBufferIndexEntry = toReadBuffer.getLong(toReadOffsetInBuffer + 8);
            toReadOffsetInBuffer += INDEX_ENTRY_SIZE;

            // Allocate a temp buffer for the event, recycle the original buffer
            if (bufferDataType.isEvent() && readMemorySegment.size() < recordLength) {
                bufferRecycler.recycle(readMemorySegment);
                readMemorySegment = MemorySegmentFactory.allocateUnpooledSegment(recordLength);
            }

            // Start reading data from the data buffer
            numBytesRead +=
                    readRecordOrEventToTargetBuffer(
                            readMemorySegment,
                            numBytesRead,
                            toReadBufferIndex,
                            toReadOffsetInBuffer,
                            recordLength);

            if (shouldReadNextBuffer(currentReadingSubpartitionId, nextReadBufferIndexEntry)) {
                break;
            }
        } while (numBytesRead < readMemorySegment.size() && bufferDataType.isBuffer());

        numTotalBytesRead += numBytesRead;
        return Pair.of(
                currentReadingSubpartitionId,
                new NetworkBuffer(readMemorySegment, bufferRecycler, bufferDataType, numBytesRead));
    }

    boolean hasRemaining() {
        return numTotalBytesRead < numTotalBytes;
    }

    void finish() {
        checkState(!isFinished, "DataBuffer is already finished.");
        isFinished = true;

        // Prepare for reading
        startReadNextSubpartitionAndUpdateIndexEntryAddress();
    }

    void release() {
        if (isReleased) {
            return;
        }
        isReleased = true;
        clearSegments();
    }

    boolean isFinished() {
        return isFinished;
    }

    boolean isReleased() {
        return isReleased;
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    private void writeIndex(int subpartitionId, int numRecordBytes, Buffer.DataType dataType) {
        MemorySegment segment = dataSegments.get(writeBufferIndex);

        // Record length takes the high 32 bits and data type takes the low 32 bits
        segment.putLong(
                writeOffsetInCurrentBuffer, ((long) numRecordBytes << 32) | dataType.ordinal());

        // Buffer index takes the high 32 bits and segment offset takes the low 32 bits
        long bufferIndexEntry = ((long) writeBufferIndex << 32) | writeOffsetInCurrentBuffer;

        // Get the previous buffer index entry
        long lastBufferIndexEntry = subpartitionLastBufferIndexEntries[subpartitionId];
        subpartitionLastBufferIndexEntries[subpartitionId] = bufferIndexEntry;

        if (lastBufferIndexEntry >= 0) {
            // Link the previous index entry of the given subpartition to the new index entry, then
            // the current index entry can be got by the pointer when reading data
            segment = dataSegments.get(getBufferIdFromBufferIndexEntry(lastBufferIndexEntry));
            segment.putLong(
                    getOffsetInBufferFromBufferIndexEntry(lastBufferIndexEntry) + 8,
                    bufferIndexEntry);
        } else {
            subpartitionFirstBufferIndexEntries[subpartitionId] = bufferIndexEntry;
        }

        // After writing record, update the buffer index and the offset
        updateWriteBufferIndexAndOffset(INDEX_ENTRY_SIZE);
    }

    private void writeRecord(ByteBuffer record) {
        while (record.hasRemaining()) {
            MemorySegment segment = dataSegments.get(writeBufferIndex);
            int toCopy = Math.min(bufferSizeBytes - writeOffsetInCurrentBuffer, record.remaining());
            segment.put(writeOffsetInCurrentBuffer, record, toCopy);

            updateWriteBufferIndexAndOffset(toCopy);
        }
    }

    private boolean allocateBuffersForRecord(int numRecordBytes) {
        int numBytesRequired = INDEX_ENTRY_SIZE + numRecordBytes;
        int availableBytes =
                writeBufferIndex == dataSegments.size()
                        ? 0
                        : bufferSizeBytes - writeOffsetInCurrentBuffer;

        // Return directly if current available bytes is enough
        if (availableBytes >= numBytesRequired) {
            return true;
        }

        // Skip the remaining free space if the available bytes is not enough for an index entry
        if (availableBytes < INDEX_ENTRY_SIZE) {
            updateWriteBufferIndexAndOffset(availableBytes);
            availableBytes = 0;
        }

        if (availableBytes + (numBuffersForSort - dataSegments.size()) * (long) bufferSizeBytes
                < numBytesRequired) {
            return false;
        }

        // Allocate exactly enough buffers for the appended record
        do {
            MemorySegment segment = freeSegments.poll();
            availableBytes += bufferSizeBytes;
            addBuffer(checkNotNull(segment));
        } while (availableBytes < numBytesRequired);

        return true;
    }

    private boolean shouldReadNextBuffer(
            int currentReadingSubpartitionId, long nextReadBufferIndexEntry) {
        if (recordRemainingBytesToRead == 0) {
            // If this buffer is the last buffer of the subpartition, start reading the next
            // subpartition.
            if (readBufferIndexEntry
                    == subpartitionLastBufferIndexEntries[currentReadingSubpartitionId]) {
                startReadNextSubpartitionAndUpdateIndexEntryAddress();
                return true;
            }
            // If this buffer is not the last buffer of the subpartition, read the next buffer
            readBufferIndexEntry = nextReadBufferIndexEntry;
        }
        return false;
    }

    private void addBuffer(MemorySegment segment) {
        if (segment.size() != bufferSizeBytes) {
            bufferRecycler.recycle(segment);
            throw new IllegalStateException("Illegal memory segment size.");
        }

        if (isReleased) {
            bufferRecycler.recycle(segment);
            throw new IllegalStateException("Sort buffer is already released.");
        }

        dataSegments.add(segment);
    }

    private void updateWriteBufferIndexAndOffset(int numBytes) {
        writeOffsetInCurrentBuffer += numBytes;

        // Use the next available free buffer if the current buffer is full
        if (writeOffsetInCurrentBuffer == bufferSizeBytes) {
            ++writeBufferIndex;
            writeOffsetInCurrentBuffer = 0;
        }
    }

    private int readRecordOrEventToTargetBuffer(
            MemorySegment targetBuffer,
            int targetBufferOffset,
            int sourceBufferIndex,
            int sourceBufferOffset,
            int recordLength) {
        if (recordRemainingBytesToRead > 0) {
            // Skip the partial record from the last read
            long position = (long) sourceBufferOffset + (recordLength - recordRemainingBytesToRead);
            sourceBufferIndex += (position / bufferSizeBytes);
            sourceBufferOffset = (int) (position % bufferSizeBytes);
        } else {
            recordRemainingBytesToRead = recordLength;
        }

        int targetSegmentSize = targetBuffer.size();
        int numBytesToRead =
                Math.min(targetSegmentSize - targetBufferOffset, recordRemainingBytesToRead);
        MemorySegment sourceSegment;
        do {
            // Read the next data buffer if all data of the current buffer has been copied
            if (sourceBufferOffset == bufferSizeBytes) {
                ++sourceBufferIndex;
                sourceBufferOffset = 0;
            }

            int sourceRemainingBytes =
                    Math.min(bufferSizeBytes - sourceBufferOffset, recordRemainingBytesToRead);
            int numBytes = Math.min(targetSegmentSize - targetBufferOffset, sourceRemainingBytes);
            sourceSegment = dataSegments.get(sourceBufferIndex);
            sourceSegment.copyTo(sourceBufferOffset, targetBuffer, targetBufferOffset, numBytes);

            recordRemainingBytesToRead -= numBytes;
            targetBufferOffset += numBytes;
            sourceBufferOffset += numBytes;
        } while ((recordRemainingBytesToRead > 0 && targetBufferOffset < targetSegmentSize));

        return numBytesToRead;
    }

    private void startReadNextSubpartitionAndUpdateIndexEntryAddress() {
        // skip the channels without any data
        while (++readingSubpartitionId < subpartitionFirstBufferIndexEntries.length) {
            if ((readBufferIndexEntry = subpartitionFirstBufferIndexEntries[readingSubpartitionId])
                    >= 0) {
                break;
            }
        }
    }

    private Buffer.DataType getBufferDataType(long lengthAndDataType) {
        return Buffer.DataType.values()[getOffsetInBufferFromBufferIndexEntry(lengthAndDataType)];
    }

    private int getRecordLength(long value) {
        return (int) (value >>> 32);
    }

    private int getBufferIdFromBufferIndexEntry(long value) {
        return (int) (value >>> 32);
    }

    private int getOffsetInBufferFromBufferIndexEntry(long value) {
        return (int) (value);
    }

    private void clearSegments() {
        for (MemorySegment segment : dataSegments) {
            bufferRecycler.recycle(segment);
        }
        dataSegments.clear();
    }
}
