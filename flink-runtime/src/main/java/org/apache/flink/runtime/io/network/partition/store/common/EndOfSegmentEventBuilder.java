package org.apache.flink.runtime.io.network.partition.store.common;

import org.apache.flink.runtime.io.network.api.EndOfSegmentEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;

import java.io.IOException;
import java.nio.ByteBuffer;

/** The builder generating the event of EndOfSegment. */
public class EndOfSegmentEventBuilder {
    public static ByteBuffer buildEndOfSegmentEvent(long segmentIndex) {
        final ByteBuffer serializedEvent;
        try {
            serializedEvent =
                    EventSerializer.toSerializedEvent(
                            new EndOfSegmentEvent(segmentIndex));
        } catch (IOException e) {
            throw new RuntimeException("Failed to build EndOfSegment.");
        }
        return serializedEvent;
    }
}
