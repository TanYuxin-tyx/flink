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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.common;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;

/** Utils to convert the Ids to Tiered Storage Ids, or vice versa. */
public class TieredStorageIdMappingUtils {

    public static TieredStoragePartitionId convertId(ResultPartitionID resultPartitionId) {
        ByteBuf byteBuf = Unpooled.buffer();
        resultPartitionId.getPartitionId().writeTo(byteBuf);
        resultPartitionId.getProducerId().writeTo(byteBuf);

        byte[] bytes = new byte[byteBuf.readableBytes()];
        byteBuf.readBytes(bytes);
        byteBuf.release();

        return new TieredStoragePartitionId(bytes);
    }

    public static ResultPartitionID convertId(TieredStoragePartitionId partitionId) {
        ByteBuf byteBuf = Unpooled.buffer();
        byteBuf.writeBytes(partitionId.getId());

        IntermediateResultPartitionID partitionID =
                IntermediateResultPartitionID.fromByteBuf(byteBuf);
        ExecutionAttemptID attemptID = ExecutionAttemptID.fromByteBuf(byteBuf);
        byteBuf.release();

        return new ResultPartitionID(partitionID, attemptID);
    }
}
