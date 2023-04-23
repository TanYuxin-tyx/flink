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

package org.apache.flink.runtime.io.network.partition.consumer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;

/** {@link SingInputGateConsumerClient} includes the logic of reading buffer from channel. */
public interface SingInputGateConsumerClient extends Closeable {

    /** Start the reader. */
    void start();

    /**
     * Get next buffer.
     *
     * @param inputChannel indicate the subpartition to read buffer.
     * @return the next buffer.
     */
    Optional<InputChannel.BufferAndAvailability> getNextBuffer(InputChannel inputChannel)
            throws IOException, InterruptedException;

    /**
     * Ask if the reader is needed to acknowledge all records are processed to upstream. If the
     * reader reads buffer through tiered store, it doesn't need to acknowledge it.
     *
     * @return if the reader is needed to acknowledge all records are processed to upstream.
     */
    boolean supportAcknowledgeUpstreamAllRecordsProcessed();
}
