package org.apache.flink.runtime.io.network.partition.hybrid.tiered.downstream;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FatalExitExceptionHandler;

import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoreUtils.generateNewSegmentPath;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoreUtils.generateSegmentFinishPath;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoreUtils.getBaseSubpartitionPath;

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

    private final Map<Integer, Boolean>[] existStatus;

    private volatile int[] requiredSegmentIds;

    private volatile int[] scanningSegmentIds;

    private volatile int[] readingSegmentIds;

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

    private InputChannel[] inputChannels;

    private Consumer<InputChannel> channelEnqueueReceiver;

    public RemoteTierMonitor(
            JobID jobID,
            List<ResultPartitionID> resultPartitionIDs,
            String baseRemoteStoragePath,
            List<Integer> subpartitionIndexes) {
        this.existStatus = new Map[subpartitionIndexes.size()];
        Arrays.fill(existStatus, new ConcurrentHashMap<>());
        this.requiredSegmentIds = new int[subpartitionIndexes.size()];
        this.scanningSegmentIds = new int[subpartitionIndexes.size()];
        this.readingSegmentIds = new int[subpartitionIndexes.size()];
        Arrays.fill(readingSegmentIds, -1);
        this.inputStreams = new FSDataInputStream[subpartitionIndexes.size()];
        this.subpartitionIndexes = subpartitionIndexes;
        this.jobID = jobID;
        this.resultPartitionIDs = resultPartitionIDs;
        this.baseRemoteStoragePath = baseRemoteStoragePath;
    }

    public void setup(InputChannel[] inputChannels, Consumer<InputChannel> channelEnqueueReceiver) {
        this.inputChannels = inputChannels;
        this.channelEnqueueReceiver = channelEnqueueReceiver;
        try {
            this.remoteFileSystem = new Path(baseRemoteStoragePath).getFileSystem();
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize the FileSystem", e);
        }
    }

    public void start() {
        monitorExecutor.scheduleAtFixedRate(this, 0, 10, TimeUnit.MILLISECONDS);
    }

    @Override
    public void run() {
        for (int channelIndex = 0; channelIndex < inputChannels.length; channelIndex++) {
            boolean isEnqueue = false;
            synchronized (lock) {
                int scanningSegmentId = scanningSegmentIds[channelIndex];
                int requiredSegmentId = requiredSegmentIds[channelIndex];
                if (scanningSegmentId <= requiredSegmentId
                        && hasSegmentId(inputChannels[channelIndex], scanningSegmentId)) {
                    scanningSegmentIds[channelIndex] = scanningSegmentId + 1;
                    isEnqueue = true;
                }
            }
            if (isEnqueue) {
                channelEnqueueReceiver.accept(inputChannels[channelIndex]);
            }
        }
    }

    public boolean isExist(int channelIndex, int segmentId) {
        return hasSegmentId(inputChannels[channelIndex], segmentId);
    }

    public void requireSegmentId(int channelIndex, int segmentId) {
        synchronized (lock) {
            requiredSegmentIds[channelIndex] = segmentId;
        }
    }

    public FSDataInputStream getInputStream(InputChannel inputChannel, int segmentId) {
        FSDataInputStream requiredInputStream = inputStreams[inputChannel.getChannelIndex()];
        if (requiredInputStream == null
                || readingSegmentIds[inputChannel.getChannelIndex()] != segmentId) {
            String baseSubpartitionPath =
                    getBaseSubpartitionPath(
                            jobID,
                            resultPartitionIDs.get(inputChannel.getChannelIndex()),
                            subpartitionIndexes.get(inputChannel.getChannelIndex()),
                            baseRemoteStoragePath,
                            inputChannel.isUpstreamBroadcastOnly());
            Path currentSegmentPath = generateNewSegmentPath(baseSubpartitionPath, segmentId);
            FSDataInputStream inputStream = null;
            try {
                inputStream = remoteFileSystem.open(currentSegmentPath);
            } catch (IOException e) {
                ExceptionUtils.rethrow(e, "Failed to open the segment path: " + currentSegmentPath);
            }
            inputStreams[inputChannel.getChannelIndex()] = inputStream;
            readingSegmentIds[inputChannel.getChannelIndex()] = segmentId;
            return inputStream;
        } else {
            return inputStreams[inputChannel.getChannelIndex()];
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

    private boolean hasSegmentId(InputChannel inputChannel, int segmentId) {
        boolean isBroadcastOnly = inputChannel.isUpstreamBroadcastOnly();
        String baseSubpartitionPath =
                getBaseSubpartitionPath(
                        jobID,
                        resultPartitionIDs.get(inputChannel.getChannelIndex()),
                        subpartitionIndexes.get(inputChannel.getChannelIndex()),
                        baseRemoteStoragePath,
                        isBroadcastOnly);
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
