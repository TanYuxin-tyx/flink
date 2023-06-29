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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.remote;

/**
 * {@link RemoteStorageScannerAvailabilityAndPriorityHelper} is used to help the {@link
 * RemoteStorageScanner} to notify the availability and priority of the reading process for the
 * specific partition and subpartition on remote storage.
 */
public interface RemoteStorageScannerAvailabilityAndPriorityHelper {

    /**
     * Notify the available and priority status of the reading process for the specific partition
     * and subpartition.
     *
     * @param channelIndex the index of input channel related to the specific partition and
     *     subpartition.
     * @param isPriority the value is to indicate the priority of reading process.
     */
    void notifyAvailableAndPriority(int channelIndex, boolean isPriority);
}
