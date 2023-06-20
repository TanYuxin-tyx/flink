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
import org.apache.flink.runtime.io.network.partition.SortBasedDataBuffer;

import org.apache.commons.lang3.tuple.Pair;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

public class SortBufferContainer {

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

    // ---------------------------------------------------------------------------------------------
    // Statistics and states
    // ---------------------------------------------------------------------------------------------

    /** Total number of bytes already appended to this sort buffer. */
    private long numTotalBytes;

    /** Total number of records already appended to this sort buffer. */
    private long numTotalRecords;

    /** Total number of bytes already read from this sort buffer. */
    private long numTotalBytesRead;

    /** Whether this sort buffer is finished. One can only read a finished sort buffer. */
    private boolean isFinished;

    /** Whether this sort buffer is released. A released sort buffer can not be used. */
    private boolean isReleased;

    // ---------------------------------------------------------------------------------------------
    // For writing
    // ---------------------------------------------------------------------------------------------

    /** Array index in the segment list of the current available buffer for writing. */
    private int writeBufferId;

    /** Next position in the current available buffer for writing. */
    private int writeOffsetInCurrentBuffer;

    // ---------------------------------------------------------------------------------------------
    // For reading
    // ---------------------------------------------------------------------------------------------

    /** Index entry address of the current record or event to be read. */
    private long readBufferIndexEntry;

    /** Record bytes remaining after last copy, which must be read first in next copy. */
    private int remainingBytesToRead;

    /** Used to index the current available channel to read data from. */
    private int readingSubpartitionId = -1;

    SortBufferContainer(
            LinkedList<MemorySegment> freeSegments,
            BufferRecycler bufferRecycler,
            int numSubpartitions,
            int bufferSizeBytes,
            int numBuffersForSort) {
        checkArgument(bufferSizeBytes > INDEX_ENTRY_SIZE, "Buffer size is too small.");
        checkArgument(numBuffersForSort > 0, "No guaranteed buffers for sort.");

        this.freeSegments = checkNotNull(freeSegments);
        this.bufferRecycler = checkNotNull(bufferRecycler);
        this.bufferSizeBytes = bufferSizeBytes;
        this.numBuffersForSort = numBuffersForSort;
        this.dataSegments = new ArrayList<>();
        this.subpartitionFirstBufferIndexEntries = new long[numSubpartitions];
        this.subpartitionLastBufferIndexEntries = new long[numSubpartitions];

        checkState(numBuffersForSort <= freeSegments.size(), "Wrong number of free segments.");
        // initialized with -1 means the corresponding channel has no data
        Arrays.fill(subpartitionFirstBufferIndexEntries, -1L);
        Arrays.fill(subpartitionLastBufferIndexEntries, -1L);
    }

    // ------------------------------------------------------------------------
    //  Called by SortBufferAccumulator
    // ------------------------------------------------------------------------

    /**
     * No partial record will be written to this {@link SortBasedDataBuffer}, which means that
     * either all data of target record will be written or nothing will be written.
     */
    boolean writeRecord(ByteBuffer source, int targetChannel, Buffer.DataType dataType) {
        checkArgument(source.hasRemaining(), "Cannot writeRecord empty data.");
        checkState(!isFinished, "Sort buffer is already finished.");
        checkState(!isReleased, "Sort buffer is already released.");

        int totalBytes = source.remaining();

        // return true directly if it can not allocate enough buffers for the given record
        if (!allocateBuffersForRecord(totalBytes)) {
            //            clearSegments();
            return true;
        }

        // writeRecord the index entry and record or event data
        writeIndex(targetChannel, totalBytes, dataType);
        writeRecord(source);

        ++numTotalRecords;
        numTotalBytes += totalBytes;

        return false;
    }

    Pair<Integer, Buffer> readBuffer(MemorySegment readMemorySegment) {
        checkState(isFinished, "Sort buffer is not ready to be read.");
        checkState(!isReleased, "Sort buffer is already released.");

        if (!hasRemaining()) {
            return null;
        }

        int numBytesCopied = 0;
        Buffer.DataType bufferDataType = Buffer.DataType.DATA_BUFFER;
        int currentReadingSubpartitionId = readingSubpartitionId;

        do {
            int toReadBufferIndex = getBufferIdFromBufferIndexEntry(readBufferIndexEntry);
            int toReadOffsetInBuffer = getOffsetInBufferFromBufferIndexEntry(readBufferIndexEntry);
            MemorySegment toReadBuffer = dataSegments.get(toReadBufferIndex);

            long lengthAndDataType = toReadBuffer.getLong(toReadOffsetInBuffer);
            int length = getBufferIdFromBufferIndexEntry(lengthAndDataType);
            Buffer.DataType dataType =
                    Buffer.DataType.values()[
                            getOffsetInBufferFromBufferIndexEntry(lengthAndDataType)];

            // return the data read directly if the next to read is an event
            if (dataType.isEvent() && numBytesCopied > 0) {
                break;
            }
            bufferDataType = dataType;

            // get the next index entry address and move the read position forward
            long nextReadBufferIndexEntry = toReadBuffer.getLong(toReadOffsetInBuffer + 8);
            toReadOffsetInBuffer += INDEX_ENTRY_SIZE;

            // allocate a temp buffer for the event if the target buffer is not big enough
            if (bufferDataType.isEvent() && readMemorySegment.size() < length) {
                bufferRecycler.recycle(readMemorySegment);
                readMemorySegment = MemorySegmentFactory.allocateUnpooledSegment(length);
            }

            numBytesCopied +=
                    copyRecordOrEvent(
                            readMemorySegment,
                            numBytesCopied,
                            toReadBufferIndex,
                            toReadOffsetInBuffer,
                            length);

            if (remainingBytesToRead == 0) {
                // move to next channel if the current channel has been finished
                if (readBufferIndexEntry
                        == subpartitionLastBufferIndexEntries[currentReadingSubpartitionId]) {
                    updateReadChannelAndIndexEntryAddress();
                    break;
                }
                readBufferIndexEntry = nextReadBufferIndexEntry;
            }
        } while (numBytesCopied < readMemorySegment.size() && bufferDataType.isBuffer());

        numTotalBytesRead += numBytesCopied;
        return Pair.of(
                currentReadingSubpartitionId,
                new NetworkBuffer(
                        readMemorySegment, bufferRecycler, bufferDataType, numBytesCopied));
    }

    long numTotalRecords() {
        return numTotalRecords;
    }

    long numTotalBytes() {
        return numTotalBytes;
    }

    boolean hasRemaining() {
        return numTotalBytesRead < numTotalBytes;
    }

    void finish() {
        checkState(!isFinished, "DataBuffer is already finished.");
        isFinished = true;

        // prepare for reading
        updateReadChannelAndIndexEntryAddress();
    }

    boolean isFinished() {
        return isFinished;
    }

    void release() {
        if (isReleased) {
            return;
        }
        isReleased = true;
        clearSegments();
    }

    boolean isReleased() {
        return isReleased;
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    private void writeIndex(int subpartitionId, int numRecordBytes, Buffer.DataType dataType) {
        MemorySegment segment = dataSegments.get(writeBufferId);

        // record length takes the high 32 bits and data type takes the low 32 bits
        segment.putLong(
                writeOffsetInCurrentBuffer, ((long) numRecordBytes << 32) | dataType.ordinal());

        // segment index takes the high 32 bits and segment offset takes the low 32 bits
        long bufferIndexEntry = ((long) writeBufferId << 32) | writeOffsetInCurrentBuffer;

        long lastBufferIndexEntry = subpartitionLastBufferIndexEntries[subpartitionId];
        subpartitionLastBufferIndexEntries[subpartitionId] = bufferIndexEntry;

        if (lastBufferIndexEntry >= 0) {
            // link the previous index entry of the given channel to the new index entry
            segment = dataSegments.get(getBufferIdFromBufferIndexEntry(lastBufferIndexEntry));
            segment.putLong(
                    getOffsetInBufferFromBufferIndexEntry(lastBufferIndexEntry) + 8,
                    bufferIndexEntry);
        } else {
            subpartitionFirstBufferIndexEntries[subpartitionId] = bufferIndexEntry;
        }

        // move the writeRecord position forward so as to writeRecord the corresponding record
        updateWriteSegmentIndexAndOffset(INDEX_ENTRY_SIZE);
    }

    private void writeRecord(ByteBuffer source) {
        while (source.hasRemaining()) {
            MemorySegment segment = dataSegments.get(writeBufferId);
            int toCopy = Math.min(bufferSizeBytes - writeOffsetInCurrentBuffer, source.remaining());
            segment.put(writeOffsetInCurrentBuffer, source, toCopy);

            // move the writeRecord position forward so as to writeRecord the remaining bytes or
            // next record
            updateWriteSegmentIndexAndOffset(toCopy);
        }
    }

    private boolean allocateBuffersForRecord(int numRecordBytes) {
        int numBytesRequired = INDEX_ENTRY_SIZE + numRecordBytes;
        int availableBytes =
                writeBufferId == dataSegments.size()
                        ? 0
                        : bufferSizeBytes - writeOffsetInCurrentBuffer;

        // return directly if current available bytes is adequate
        if (availableBytes >= numBytesRequired) {
            return true;
        }

        // skip the remaining free space if the available bytes is not enough for an index entry
        if (availableBytes < INDEX_ENTRY_SIZE) {
            updateWriteSegmentIndexAndOffset(availableBytes);
            availableBytes = 0;
        }

        if (availableBytes + (numBuffersForSort - dataSegments.size()) * (long) bufferSizeBytes
                < numBytesRequired) {
            return false;
        }

        // allocate exactly enough buffers for the appended record
        do {
            MemorySegment segment = freeSegments.poll();
            availableBytes += bufferSizeBytes;
            addBuffer(checkNotNull(segment));
        } while (availableBytes < numBytesRequired);

        return true;
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

    private void updateWriteSegmentIndexAndOffset(int numBytes) {
        writeOffsetInCurrentBuffer += numBytes;

        // using the next available free buffer if the current is full
        if (writeOffsetInCurrentBuffer == bufferSizeBytes) {
            ++writeBufferId;
            writeOffsetInCurrentBuffer = 0;
        }
    }

    private int copyRecordOrEvent(
            MemorySegment targetSegment,
            int targetSegmentOffset,
            int sourceSegmentIndex,
            int sourceSegmentOffset,
            int recordLength) {
        if (remainingBytesToRead > 0) {
            // skip the data already read if there is remaining partial record after the previous
            // copy
            long position = (long) sourceSegmentOffset + (recordLength - remainingBytesToRead);
            sourceSegmentIndex += (position / bufferSizeBytes);
            sourceSegmentOffset = (int) (position % bufferSizeBytes);
        } else {
            remainingBytesToRead = recordLength;
        }

        int targetSegmentSize = targetSegment.size();
        int numBytesToCopy =
                Math.min(targetSegmentSize - targetSegmentOffset, remainingBytesToRead);
        MemorySegment sourceSegment;
        do {
            // move to next data buffer if all data of the current buffer has been copied
            if (sourceSegmentOffset == bufferSizeBytes) {
                ++sourceSegmentIndex;
                sourceSegmentOffset = 0;
            }

            int sourceRemainingBytes =
                    Math.min(bufferSizeBytes - sourceSegmentOffset, remainingBytesToRead);
            int numBytes = Math.min(targetSegmentSize - targetSegmentOffset, sourceRemainingBytes);
            sourceSegment = dataSegments.get(sourceSegmentIndex);
            sourceSegment.copyTo(sourceSegmentOffset, targetSegment, targetSegmentOffset, numBytes);

            remainingBytesToRead -= numBytes;
            targetSegmentOffset += numBytes;
            sourceSegmentOffset += numBytes;
        } while ((remainingBytesToRead > 0 && targetSegmentOffset < targetSegmentSize));

        return numBytesToCopy;
    }

    private void updateReadChannelAndIndexEntryAddress() {
        // skip the channels without any data
        while (++readingSubpartitionId < subpartitionFirstBufferIndexEntries.length) {
            if ((readBufferIndexEntry = subpartitionFirstBufferIndexEntries[readingSubpartitionId])
                    >= 0) {
                break;
            }
        }
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
