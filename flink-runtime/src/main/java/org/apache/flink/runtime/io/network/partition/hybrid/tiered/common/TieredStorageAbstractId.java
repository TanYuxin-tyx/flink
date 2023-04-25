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

import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.shuffle.TieredStoreUtils.bytesToHexString;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.shuffle.TieredStoreUtils.randomBytes;
import static org.apache.flink.util.Preconditions.checkArgument;

/** The abstract unique identification for the Tiered Storage. */
public class TieredStorageAbstractId implements TieredStorageDataIdentifier, Serializable {

    private static final long serialVersionUID = -948472905048472823L;

    /** ID represented by a byte array. */
    protected final byte[] id;

    /** Pre-calculated hash-code for acceleration. */
    protected final int hashCode;

    public TieredStorageAbstractId(byte[] id) {
        checkArgument(id != null, "Must be not null.");

        this.id = id;
        this.hashCode = Arrays.hashCode(id);
    }

    public TieredStorageAbstractId(int length) {
        checkArgument(length > 0, "Must be positive.");

        this.id = randomBytes(length);
        this.hashCode = Arrays.hashCode(id);
    }

    public byte[] getId() {
        return id;
    }

    /** Serializes this ID to the target {@link ByteBuf}. */
    public void writeTo(ByteBuf byteBuf) {
        byteBuf.writeInt(id.length);
        byteBuf.writeBytes(id);
    }

    /** Persists this ID to the target {@link DataOutput}. */
    public void writeTo(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(id.length);
        dataOutput.write(id);
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }

        if (that == null || getClass() != that.getClass()) {
            return false;
        }

        TieredStorageAbstractId thatID = (TieredStorageAbstractId) that;
        return hashCode == thatID.hashCode && Arrays.equals(id, thatID.id);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public String toString() {
        return "TieredStorageAbstractId{" + "ID=" + bytesToHexString(id) + '}';
    }
}
