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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.file;

import org.apache.flink.runtime.io.network.partition.ResultPartitionID;

/**
 * The partition file in the hash mode. In this mode, each segment of one subpartition is written to
 * an independent file.
 */
public class HashPartitionFile {

    public static HashPartitionFileWriter createPartitionFileWriter(
            String dataFilePath, int numSubpartitions, ResultPartitionID resultPartitionID) {
        return new HashPartitionFileWriter(dataFilePath, numSubpartitions, resultPartitionID);
    }

    public static HashPartitionFileReader createPartitionFileReader(
            String dataFilePath, Boolean isBroadcast) {
        return new HashPartitionFileReader(dataFilePath, isBroadcast);
    }
}
