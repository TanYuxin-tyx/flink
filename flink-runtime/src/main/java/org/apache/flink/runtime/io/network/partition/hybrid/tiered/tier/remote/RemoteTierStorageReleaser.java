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

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.common.TierStorageReleaser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.common.TieredStoreUtils.deletePath;

public class RemoteTierStorageReleaser implements TierStorageReleaser {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteTierStorageReleaser.class);

    private final Path toReleasePath;

    public RemoteTierStorageReleaser(@Nullable Path toReleasePath) {
        this.toReleasePath = toReleasePath;
    }

    @Override
    public void releaseTierStorage() {
        try {
            deletePath(toReleasePath);
        } catch (IOException e) {
            LOG.error("Failed to delete shuffle files.", e);
        }
    }
}
