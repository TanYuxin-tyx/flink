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

package org.apache.flink.test.runtime;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.configuration.BatchExecutionOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.testutils.junit.utils.TempDirUtils;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Base class for batch shuffle related tiered store IT tests. */
public class TieredStorageBatchShuffleITCaseBase {

    private static final String RECORD = "batch shuffle test";

    private static final int NUM_TASK_MANAGERS = 3;

    private static final int PARALLELISM = 10;

    private static final int[] NUM_RECEIVED_RECORDS = new int[PARALLELISM];

    private static int numParallelism = PARALLELISM;

    private static int[] numReceivedRecords = NUM_RECEIVED_RECORDS;

    private static Path tmpDir;

    @BeforeAll
    static void setup(@TempDir Path path) throws Exception {
        tmpDir = TempDirUtils.newFolder(path, UUID.randomUUID().toString()).toPath();
    }

    protected JobGraph createJobGraph(
            int numRecordsToSend, boolean failExecution, Configuration configuration) {
        return createJobGraph(numRecordsToSend, failExecution, false, configuration, false);
    }

    protected JobGraph createJobGraph(
            int numRecordsToSend,
            boolean failExecution,
            Configuration configuration,
            boolean isBroadcast) {
        return createJobGraph(numRecordsToSend, failExecution, false, configuration, isBroadcast);
    }

    protected JobGraph createJobGraph(
            int numRecordsToSend,
            boolean failExecution,
            boolean deletePartitionFile,
            Configuration configuration,
            boolean isBroadcast) {
        return createJobGraph(
                numRecordsToSend,
                failExecution,
                deletePartitionFile,
                configuration,
                -1,
                isBroadcast);
    }

    protected JobGraph createJobGraph(
            int numRecordsToSend,
            boolean failExecution,
            boolean deletePartitionFile,
            Configuration configuration,
            int parallelism) {
        return createJobGraph(
                numRecordsToSend,
                failExecution,
                deletePartitionFile,
                configuration,
                parallelism,
                false);
    }

    protected JobGraph createJobGraph(
            int numRecordsToSend,
            boolean failExecution,
            boolean deletePartitionFile,
            Configuration configuration,
            int parallelism,
            boolean isBroadcast) {
        if (parallelism > 0) {
            numParallelism = parallelism;
            numReceivedRecords = new int[parallelism];
        }
        configuration.setBoolean(BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_ENABLED, false);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, 0L));
        env.setParallelism(numParallelism);

        DataStream<String> source =
                new DataStreamSource<>(
                        env,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        new StreamSource<>(new StringSource(numRecordsToSend)),
                        true,
                        "source",
                        Boundedness.BOUNDED);
        if (isBroadcast) {
            source.broadcast()
                    .map(value -> value)
                    .shuffle()
                    .addSink(new VerifySink(failExecution, deletePartitionFile));
        } else {
            source.rebalance()
                    .map(value -> value)
                    .shuffle()
                    .addSink(new VerifySink(failExecution, deletePartitionFile));
        }

        StreamGraph streamGraph = env.getStreamGraph();
        streamGraph.setJobType(JobType.BATCH);
        return StreamingJobGraphGenerator.createJobGraph(streamGraph);
    }

    protected Configuration getConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set(CoreOptions.TMP_DIRS, tmpDir.toString());
        configuration.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        configuration.set(NettyShuffleEnvironmentOptions.NETWORK_REQUEST_BACKOFF_MAX, 100);
        return configuration;
    }

    protected void executeJob(JobGraph jobGraph, Configuration configuration, int numRecordsToSend)
            throws Exception {
        executeJob(jobGraph, configuration, numRecordsToSend, false);
    }

    protected void executeJob(
            JobGraph jobGraph,
            Configuration configuration,
            int numRecordsToSend,
            boolean isBroadcast)
            throws Exception {
        JobGraphRunningUtil.execute(jobGraph, configuration, NUM_TASK_MANAGERS, numParallelism);
        checkAllDataReceived(numRecordsToSend, isBroadcast);
    }

    private void checkAllDataReceived(int numRecordsToSend, boolean isBroadcast) {
        int expectedRecordsNum =
                isBroadcast
                        ? numRecordsToSend * numParallelism * numParallelism
                        : numRecordsToSend * numParallelism;
        assertThat(Arrays.stream(numReceivedRecords).sum()).isEqualTo(expectedRecordsNum);
    }

    private static class StringSource implements ParallelSourceFunction<String> {
        private static final Logger LOG = LoggerFactory.getLogger(StringSource.class);
        private volatile boolean isRunning = true;
        private int numRecordsToSend;

        StringSource(int numRecordsToSend) {
            this.numRecordsToSend = numRecordsToSend;
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while (isRunning && numRecordsToSend-- > 0) {
                ctx.collect(RECORD);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    private static class VerifySink extends RichSinkFunction<String> {
        private final boolean failExecution;

        private final boolean deletePartitionFile;

        VerifySink(boolean failExecution, boolean deletePartitionFile) {
            this.failExecution = failExecution;
            this.deletePartitionFile = deletePartitionFile;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            numReceivedRecords[getRuntimeContext().getIndexOfThisSubtask()] = 0;
            if (getRuntimeContext().getAttemptNumber() > 0
                    || getRuntimeContext().getIndexOfThisSubtask() != 0) {
                return;
            }

            if (deletePartitionFile) {
                synchronized (BlockingShuffleITCase.class) {
                    deleteFiles(tmpDir.toFile());
                }
            }

            if (failExecution) {
                throw new RuntimeException("expected exception.");
            }
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            numReceivedRecords[getRuntimeContext().getIndexOfThisSubtask()]++;
            assertThat(value).isEqualTo(RECORD);
        }

        private void deleteFiles(File root) throws IOException {
            File[] files = root.listFiles();
            if (files == null || files.length == 0) {
                return;
            }

            for (File file : files) {
                if (!file.isDirectory()) {
                    Files.deleteIfExists(file.toPath());
                } else {
                    deleteFiles(file);
                }
            }
        }
    }
}
