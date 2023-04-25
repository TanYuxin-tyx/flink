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

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import java.io.DataInput;
import java.io.IOException;

import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.shuffle.TieredStoreUtils.bytesToHexString;

/**
 * Identifier of a partition.
 *
 * <p>A partition is equivalent to a result partition in Flink.
 */
public class TieredStoragePartitionId extends TieredStorageAbstractId {

    private static final long serialVersionUID = -483241996628471038L;

    public TieredStoragePartitionId(byte[] resultPartitionID) {
        super(resultPartitionID);
    }

    /**
     * Deserializes and creates an {@link TieredStoragePartitionId} from the given {@link ByteBuf}.
     */
    public static TieredStoragePartitionId readFrom(ByteBuf byteBuf) {
        byte[] bytes = new byte[byteBuf.readInt()];
        byteBuf.readBytes(bytes);

        return new TieredStoragePartitionId(bytes);
    }

    /**
     * Deserializes and creates an {@link TieredStoragePartitionId} from the given {@link
     * DataInput}.
     */
    public static TieredStoragePartitionId readFrom(DataInput dataInput) throws IOException {
        byte[] bytes = new byte[dataInput.readInt()];
        dataInput.readFully(bytes);
        return new TieredStoragePartitionId(bytes);
    }

    @Override
    public String toString() {
        return "TieredStoragePartitionId{" + "ID=" + bytesToHexString(id) + '}';
    }
}
