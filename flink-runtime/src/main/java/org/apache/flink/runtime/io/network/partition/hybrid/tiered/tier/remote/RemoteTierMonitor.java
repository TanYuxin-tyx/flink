package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.remote;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FatalExitExceptionHandler;

import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageUtils.generateNewSegmentPath;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageUtils.generateSegmentFinishPath;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageUtils.getBaseSubpartitionPath;

/**
 * The {@link RemoteTierMonitor} is the monitor to scan the existing status of shuffle data stored
 * in Remote Tier.
 */
public class RemoteTierMonitor implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteTierMonitor.class);

    private final Object lock = new Object();

    private final JobID jobID;

    private final List<ResultPartitionID> resultPartitionIDs;

    private final String baseRemoteStoragePath;

    private final int[] requiredSegmentIds;

    private final int[] scanningSegmentIds;

    private final int[] readingSegmentIds;

    private final FSDataInputStream[] inputStreams;

    private final List<Integer> subpartitionIndexes;

    private FileSystem remoteFileSystem;

    private final ScheduledExecutorService monitorExecutor =
            Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder()
                            .setNameFormat("tiered store remote monitor")
                            // It is more appropriate to use task fail over than exit JVM here,
                            // but the task thread will bring some extra overhead to check the
                            // exception information set by other thread. As the spiller thread will
                            // not encounter exceptions in most cases, we temporarily choose the
                            // form of fatal error to deal except thrown by spiller thread.
                            .setUncaughtExceptionHandler(FatalExitExceptionHandler.INSTANCE)
                            .build());

    private final int numInputChannels;

    private final boolean isUpstreamBroadcast;

    private final Consumer<Integer> channelEnqueueReceiver;

    public RemoteTierMonitor(
            JobID jobID,
            List<ResultPartitionID> resultPartitionIDs,
            String baseRemoteStoragePath,
            List<Integer> subpartitionIndexes,
            int numInputChannels,
            boolean isUpstreamBroadcast,
            Consumer<Integer> channelEnqueueReceiver) {
        this.requiredSegmentIds = new int[subpartitionIndexes.size()];
        this.scanningSegmentIds = new int[subpartitionIndexes.size()];
        this.readingSegmentIds = new int[subpartitionIndexes.size()];
        Arrays.fill(readingSegmentIds, -1);
        this.inputStreams = new FSDataInputStream[subpartitionIndexes.size()];
        this.subpartitionIndexes = subpartitionIndexes;
        this.jobID = jobID;
        this.resultPartitionIDs = resultPartitionIDs;
        this.baseRemoteStoragePath = baseRemoteStoragePath;
        this.isUpstreamBroadcast = isUpstreamBroadcast;
        this.numInputChannels = numInputChannels;
        this.channelEnqueueReceiver = channelEnqueueReceiver;
        try {
            this.remoteFileSystem = new Path(baseRemoteStoragePath).getFileSystem();
        } catch (IOException e) {
            ExceptionUtils.rethrow(e, "Failed to initialize the FileSystem");
        }
    }

    public void start() {
        monitorExecutor.scheduleAtFixedRate(this, 0, 10, TimeUnit.MILLISECONDS);
    }

    @Override
    public void run() {
        for (int channelIndex = 0; channelIndex < numInputChannels; channelIndex++) {
            boolean isEnqueue = false;
            synchronized (lock) {
                int scanningSegmentId = scanningSegmentIds[channelIndex];
                int requiredSegmentId = requiredSegmentIds[channelIndex];
                if (scanningSegmentId <= requiredSegmentId
                        && hasSegmentId(channelIndex, scanningSegmentId)) {
                    scanningSegmentIds[channelIndex] = scanningSegmentId + 1;
                    isEnqueue = true;
                }
            }
            if (isEnqueue) {
                channelEnqueueReceiver.accept(channelIndex);
            }
        }
    }

    public boolean isExist(int channelIndex, int segmentId) {
        return hasSegmentId(channelIndex, segmentId);
    }

    public void requireSegmentId(int channelIndex, int segmentId) {
        synchronized (lock) {
            requiredSegmentIds[channelIndex] = segmentId;
        }
    }

    public FSDataInputStream getInputStream(int channelIndex, int segmentId) {
        FSDataInputStream requiredInputStream = inputStreams[channelIndex];
        if (requiredInputStream == null || readingSegmentIds[channelIndex] != segmentId) {
            String baseSubpartitionPath =
                    getBaseSubpartitionPath(
                            jobID,
                            resultPartitionIDs.get(channelIndex),
                            subpartitionIndexes.get(channelIndex),
                            baseRemoteStoragePath,
                            isUpstreamBroadcast);
            Path currentSegmentPath = generateNewSegmentPath(baseSubpartitionPath, segmentId);
            FSDataInputStream inputStream = null;
            try {
                inputStream = remoteFileSystem.open(currentSegmentPath);
            } catch (IOException e) {
                ExceptionUtils.rethrow(e, "Failed to open the segment path: " + currentSegmentPath);
            }
            inputStreams[channelIndex] = inputStream;
            readingSegmentIds[channelIndex] = segmentId;
            return inputStream;
        } else {
            return inputStreams[channelIndex];
        }
    }

    public void close() {
        try {
            monitorExecutor.shutdown();
            if (!monitorExecutor.awaitTermination(5L, TimeUnit.MINUTES)) {
                throw new TimeoutException("Shutdown spilling thread timeout.");
            }
        } catch (InterruptedException | TimeoutException e) {
            ExceptionUtils.rethrow(e, "Failed to close.");
        }
    }

    private boolean hasSegmentId(int channelIndex, int segmentId) {
        String baseSubpartitionPath =
                getBaseSubpartitionPath(
                        jobID,
                        resultPartitionIDs.get(channelIndex),
                        subpartitionIndexes.get(channelIndex),
                        baseRemoteStoragePath,
                        isUpstreamBroadcast);
        Path currentSegmentFinishPath = generateSegmentFinishPath(baseSubpartitionPath, segmentId);
        return isPathExist(currentSegmentFinishPath);
    }

    private boolean isPathExist(Path path) {
        try {
            return path.getFileSystem().exists(path);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to check the existing state of segment path: " + path, e);
        }
    }
}
