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

package org.apache.flink.runtime.io.network.api;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.RuntimeEvent;

import java.io.IOException;

/** EndOfSegmentEvent is used to notify the downstream switch tiers in Tiered Store shuffle mode. */
public class EndOfSegmentEvent extends RuntimeEvent {

    private final long segmentId;

    public EndOfSegmentEvent(long segmentId) {
        this.segmentId = segmentId;
    }

    public long getSegmentId() {
        return segmentId;
    }

    @Override
    public void write(DataOutputView out) throws IOException {
        throw new UnsupportedOperationException("This method should never be called");
    }

    @Override
    public void read(DataInputView in) throws IOException {
        throw new UnsupportedOperationException("This method should never be called");
    }

    // ------------------------------------------------------------------------

    @Override
    public int hashCode() {
        return (int) (segmentId ^ (segmentId >>> 32));
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        } else if (other == null || other.getClass() != EndOfSegmentEvent.class) {
            return false;
        } else {
            EndOfSegmentEvent that = (EndOfSegmentEvent) other;
            return that.segmentId == this.segmentId;
        }
    }

    @Override
    public String toString() {
        return String.format("EndOfSegmentEvent, ID: %d", segmentId);
    }
}
