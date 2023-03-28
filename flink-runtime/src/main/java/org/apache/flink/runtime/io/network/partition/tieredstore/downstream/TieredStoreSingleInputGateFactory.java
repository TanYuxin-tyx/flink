package org.apache.flink.runtime.io.network.partition.tieredstore.downstream;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.memory.MemorySegmentProvider;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.IndexRange;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.TaskEventPublisher;
import org.apache.flink.runtime.io.network.buffer.BufferDecompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.metrics.InputChannelMetrics;
import org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.GateBuffersSpec;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateFactory;
import org.apache.flink.runtime.io.network.partition.tieredstore.downstream.common.TieredStoreReader;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleIOOwnerContext;
import org.apache.flink.runtime.taskmanager.NettyShuffleEnvironmentConfiguration;
import org.apache.flink.runtime.throughput.BufferDebloater;
import org.apache.flink.runtime.throughput.ThroughputCalculator;
import org.apache.flink.util.clock.SystemClock;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.runtime.io.network.partition.consumer.InputGateSpecUtils.createGateBuffersSpec;

/** Factory for {@link TieredStoreSingleInputGate} to use in {@link NettyShuffleEnvironment}. */
public class TieredStoreSingleInputGateFactory extends SingleInputGateFactory {

    private final String baseRemoteStoragePath;

    public TieredStoreSingleInputGateFactory(
            @Nonnull ResourceID taskExecutorResourceId,
            @Nonnull NettyShuffleEnvironmentConfiguration networkConfig,
            @Nonnull ConnectionManager connectionManager,
            @Nonnull ResultPartitionManager partitionManager,
            @Nonnull TaskEventPublisher taskEventPublisher,
            @Nonnull NetworkBufferPool networkBufferPool,
            @Nullable String baseRemoteStoragePath) {
        super(
                taskExecutorResourceId,
                networkConfig,
                connectionManager,
                partitionManager,
                taskEventPublisher,
                networkBufferPool);
        this.baseRemoteStoragePath = baseRemoteStoragePath;
    }

    private SingleInputGate createInputGate(
            String owningTaskName,
            int gateIndex,
            List<Integer> subpartitionIndexes,
            IntermediateDataSetID consumedResultId,
            ResultPartitionType consumedPartitionType,
            IndexRange subpartitionIndexRange,
            int numberOfInputChannels,
            PartitionProducerStateProvider partitionProducerStateProvider,
            SupplierWithException<BufferPool, IOException> bufferPoolFactory,
            @Nullable BufferDecompressor bufferDecompressor,
            MemorySegmentProvider memorySegmentProvider,
            int segmentSize,
            ThroughputCalculator throughputCalculator,
            @Nullable BufferDebloater bufferDebloater,
            JobID jobID,
            List<ResultPartitionID> resultPartitionIDs) {

        TieredStoreReader tieredStoreReader =
                new TieredStoreReaderImpl(
                        jobID,
                        resultPartitionIDs,
                        memorySegmentProvider,
                        subpartitionIndexes,
                        baseRemoteStoragePath,
                        numberOfInputChannels);

        return new SingleInputGate(
                owningTaskName,
                gateIndex,
                consumedResultId,
                consumedPartitionType,
                subpartitionIndexRange,
                numberOfInputChannels,
                partitionProducerStateProvider,
                bufferPoolFactory,
                bufferDecompressor,
                memorySegmentProvider,
                segmentSize,
                throughputCalculator,
                bufferDebloater,
                tieredStoreReader);
    }

    @Override
    public SingleInputGate create(
            @Nonnull ShuffleIOOwnerContext owner,
            int gateIndex,
            @Nonnull InputGateDeploymentDescriptor igdd,
            @Nonnull PartitionProducerStateProvider partitionProducerStateProvider,
            @Nonnull InputChannelMetrics metrics) {
        GateBuffersSpec gateBuffersSpec =
                createGateBuffersSpec(
                        maxRequiredBuffersPerGate,
                        configuredNetworkBuffersPerChannel,
                        floatingNetworkBuffersPerGate,
                        igdd.getConsumedPartitionType(),
                        calculateNumChannels(
                                igdd.getShuffleDescriptors().length,
                                igdd.getConsumedSubpartitionIndexRange()),
                        enableTieredStoreForHybridShuffle);
        SupplierWithException<BufferPool, IOException> bufferPoolFactory =
                createBufferPoolFactory(
                        networkBufferPool,
                        gateBuffersSpec.getRequiredFloatingBuffers(),
                        gateBuffersSpec.getTotalFloatingBuffers());

        BufferDecompressor bufferDecompressor = null;
        if (igdd.getConsumedPartitionType().supportCompression()
                && batchShuffleCompressionEnabled) {
            bufferDecompressor = new BufferDecompressor(networkBufferSize, compressionCodec);
        }

        final String owningTaskName = owner.getOwnerName();
        final MetricGroup networkInputGroup = owner.getInputGroup();

        IndexRange subpartitionIndexRange = igdd.getConsumedSubpartitionIndexRange();
        ShuffleDescriptor[] shuffleDescriptors = igdd.getShuffleDescriptors();
        List<ResultPartitionID> resultPartitionIDs = new ArrayList<>();
        List<Integer> subpartitionIndexes = new ArrayList<>();
        for (ShuffleDescriptor shuffleDescriptor : shuffleDescriptors) {
            for (int subpartitionIndex = subpartitionIndexRange.getStartIndex();
                    subpartitionIndex <= subpartitionIndexRange.getEndIndex();
                    ++subpartitionIndex) {
                resultPartitionIDs.add(shuffleDescriptor.getResultPartitionID());
                subpartitionIndexes.add(subpartitionIndex);
            }
        }
        SingleInputGate inputGate =
                createInputGate(
                        owningTaskName,
                        gateIndex,
                        subpartitionIndexes,
                        igdd.getConsumedResultId(),
                        igdd.getConsumedPartitionType(),
                        subpartitionIndexRange,
                        calculateNumChannels(
                                igdd.getShuffleDescriptors().length, subpartitionIndexRange),
                        partitionProducerStateProvider,
                        bufferPoolFactory,
                        bufferDecompressor,
                        networkBufferPool,
                        networkBufferSize,
                        new ThroughputCalculator(SystemClock.getInstance()),
                        maybeCreateBufferDebloater(
                                owningTaskName, gateIndex, networkInputGroup.addGroup(gateIndex)),
                        owner.getJobID(),
                        resultPartitionIDs);

        createInputChannels(
                owningTaskName, igdd, inputGate, subpartitionIndexRange, gateBuffersSpec, metrics);
        return inputGate;
    }
}
