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

package org.apache.flink.runtime.io.disk;

import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.ShutdownHookUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The manager used for creating/deleting file channels based on config temp dirs. The manager will
 * reserve some disk space to avoid no more disk space exceptions.
 */
public class FileChannelManagerReserveSpaceImpl implements FileChannelManager {

    private static final Logger LOG =
            LoggerFactory.getLogger(FileChannelManagerReserveSpaceImpl.class);

    private static final float DEFAULT_RESERVE_DISK_SPACE_RATIO = 0.01f;

    /** The temporary directories for files. */
    private final File[] paths;

    /** A random number generator for the anonymous Channel IDs. */
    private final Random random;

    /** The number of the next path to use. */
    private final AtomicLong nextPath = new AtomicLong(0);

    /** Prefix of the temporary directories to create. */
    private final String prefix;

    private final long configuredMinDiskReserveBytes;

    /**
     * Flag to signal that the file channel manager has been shutdown already. The flag should
     * support concurrent access for cases like multiple shutdown hooks.
     */
    private final AtomicBoolean isShutdown = new AtomicBoolean();

    /** Shutdown hook to make sure that the directories are removed on exit. */
    private final Thread shutdownHook;

    public FileChannelManagerReserveSpaceImpl(
            String[] tempDirs, String prefix, long configuredMinDiskReserveBytes) {
        checkNotNull(tempDirs, "The temporary directories must not be null.");
        checkArgument(tempDirs.length > 0, "The temporary directories must not be empty.");

        this.random = new Random();
        this.prefix = prefix;
        this.configuredMinDiskReserveBytes = configuredMinDiskReserveBytes;

        shutdownHook =
                ShutdownHookUtil.addShutdownHook(
                        this, String.format("%s-%s", getClass().getSimpleName(), prefix), LOG);

        // Creates directories after registering shutdown hook to ensure the directories can be
        // removed if required.
        this.paths = createFiles(tempDirs, prefix);
    }

    private static File[] createFiles(String[] tempDirs, String prefix) {
        File[] files = new File[tempDirs.length];
        for (int i = 0; i < tempDirs.length; i++) {
            File baseDir = new File(tempDirs[i]);
            String subfolder = String.format("flink-%s-%s", prefix, UUID.randomUUID().toString());
            File storageDir = new File(baseDir, subfolder);

            if (!storageDir.exists() && !storageDir.mkdirs()) {
                throw new RuntimeException(
                        "Could not create storage directory for FileChannelManager: "
                                + storageDir.getAbsolutePath());
            }
            files[i] = storageDir;

            LOG.debug(
                    "FileChannelManager uses directory {} for spill files.",
                    storageDir.getAbsolutePath());
        }
        return files;
    }

    @Override
    public FileIOChannel.ID createChannel() {
        checkState(!isShutdown.get(), "File channel manager has shutdown.");

        int num = (int) (nextPath.getAndIncrement() % paths.length);
        return new FileIOChannel.ID(paths[num], num, random);
    }

    @Override
    public FileIOChannel.Enumerator createChannelEnumerator() {
        checkState(!isShutdown.get(), "File channel manager has shutdown.");

        return new FileIOChannel.Enumerator(paths, random);
    }

    @Override
    public File[] getPaths() {
        checkState(!isShutdown.get(), "File channel manager has shutdown.");

        return Arrays.copyOf(paths, paths.length);
    }

    @Override
    public boolean hasMoreUsableSpace() {
        for (File filePath : paths) {
            if (filePath.getUsableSpace() > getEffectiveMinDiskReserveBytes(filePath)) {
                return true;
            }
        }
        return false;
    }

    private long getEffectiveMinDiskReserveBytes(File filePath) {
        return configuredMinDiskReserveBytes > 0
                ? Math.max(
                        configuredMinDiskReserveBytes,
                        (long) (filePath.getTotalSpace() * DEFAULT_RESERVE_DISK_SPACE_RATIO))
                : 0;
    }

    /** Remove all the temp directories. */
    @Override
    public void close() throws Exception {
        // Marks shutdown and exits if it has already shutdown.
        if (!isShutdown.compareAndSet(false, true)) {
            return;
        }

        IOUtils.closeAll(
                Arrays.stream(paths)
                        .filter(File::exists)
                        .map(FileChannelManagerReserveSpaceImpl::getFileCloser)
                        .collect(Collectors.toList()));

        ShutdownHookUtil.removeShutdownHook(
                shutdownHook, String.format("%s-%s", getClass().getSimpleName(), prefix), LOG);
    }

    private static AutoCloseable getFileCloser(File path) {
        return () -> {
            try {
                FileUtils.deleteDirectory(path);
                LOG.info(
                        "FileChannelManager removed spill file directory {}",
                        path.getAbsolutePath());
            } catch (IOException e) {
                String errorMessage =
                        String.format(
                                "FileChannelManager failed to properly clean up temp file directory: %s",
                                path);
                throw new IOException(errorMessage, e);
            }
        };
    }
}