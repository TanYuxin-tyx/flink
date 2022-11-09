//package org.apache.flink.runtime.io.network.partition.consumer.tier.history;
//
//import org.apache.flink.api.common.JobID;
//import org.apache.flink.core.memory.MemorySegmentProvider;
//import org.apache.flink.runtime.io.network.api.EndOfSegmentEvent;
//import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
//import org.apache.flink.runtime.io.network.buffer.Buffer;
//import org.apache.flink.runtime.io.network.buffer.BufferPool;
//import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
//import org.apache.flink.runtime.io.network.partition.PrioritizedDeque;
//import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
//import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
//import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
//import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
//import org.apache.flink.runtime.io.network.partition.consumer.tier.TieredStoreSingleInputGate;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.IOException;
//import java.util.Arrays;
//import java.util.List;
//import java.util.Optional;
//import java.util.concurrent.ExecutionException;
//
//import static org.apache.flink.util.Preconditions.checkNotNull;
//import static org.apache.flink.util.Preconditions.checkState;
//
///** The data fetcher client for Local and Dfs Tier. */
//public class LocalDfsDataFetcherClient implements DataFetcherClient {
//
//    private static final Logger LOG = LoggerFactory.getLogger(TieredStoreSingleInputGate.class);
//
//    private final PrioritizedDeque<InputChannel> inputChannelsWithData;
//
//    private final long[] inputChannelConsumedSegmentIds;
//
//    private final boolean[] consumedFromDfs;
//
//    private final FetcherDataQueue fetcherDataQueue = new DfsFetcherDataQueue();
//
//    private final JobID jobID;
//
//    private final List<ResultPartitionID> resultPartitionIDs;
//
//    private final MemorySegmentProvider memorySegmentProvider;
//
//    private final int subpartitionIndex;
//
//    private final String baseDfsPath;
//
//    private final SingleInputGate singleInputGate;
//
//    public LocalDfsDataFetcherClient(
//            SingleInputGate singleInputGate,
//            int numInputChannels,
//            PrioritizedDeque<InputChannel> inputChannelsWithData,
//            JobID jobID,
//            List<ResultPartitionID> resultPartitionIDs,
//            MemorySegmentProvider memorySegmentProvider,
//            int subpartitionIndex,
//            String baseDfsPath) {
//        this.singleInputGate = singleInputGate;
//        this.inputChannelsWithData = inputChannelsWithData;
//        this.jobID = jobID;
//        this.resultPartitionIDs = resultPartitionIDs;
//        this.memorySegmentProvider = memorySegmentProvider;
//        this.subpartitionIndex = subpartitionIndex;
//        this.baseDfsPath = checkNotNull(baseDfsPath);
//        this.inputChannelConsumedSegmentIds = new long[numInputChannels];
//        this.consumedFromDfs = new boolean[numInputChannels];
//        Arrays.fill(inputChannelConsumedSegmentIds, -1L);
//        Arrays.fill(consumedFromDfs, false);
//    }
//
//    @Override
//    public void setup() throws IOException {
//        BufferPool bufferPool =
//                ((NetworkBufferPool) memorySegmentProvider).createBufferPool(10, 100);
//        try {
//            fetcherDataQueue.setup(
//                    this.jobID,
//                    this.resultPartitionIDs,
//                    bufferPool,
//                    subpartitionIndex,
//                    baseDfsPath);
//        } catch (InterruptedException e) {
//            throw new RuntimeException("Failed to set up DfsFetcherDataQueue.");
//        }
//    }
//
//    // @Override
//    // public Optional<InputGate.InputWithData<InputChannel, InputChannel.BufferAndAvailability>>
//    //        waitAndGetNextData(boolean blocking) throws IOException, InterruptedException {
//    //    while (true) {
//    //        synchronized (inputChannelsWithData) {
//    //            Optional<InputChannel.BufferAndAvailability> bufferAndAvailabilityOpt;
//    //            InputChannel inputChannel;
//    //            // If there is data in fetcherDataQueue, get data from it
//    //            if (fetcherDataQueue.getState() == FetcherDataQueueState.RUNNING) {
//    //                bufferAndAvailabilityOpt = fetcherDataQueue.getNextBuffer(true);
//    //                inputChannel =
//    // checkNotNull(fetcherDataQueue.getCurrentChannel().orElse(null));
//    //                if (!bufferAndAvailabilityOpt.isPresent()) {
//    //                    fetcherDataQueue.setState(FetcherDataQueueState.WAITING);
//    //                    continue;
//    //                }
//    //                final InputChannel.BufferAndAvailability bufferAndAvailability =
//    //                        bufferAndAvailabilityOpt.get();
//    //                if (bufferAndAvailability.moreAvailable()) {
//    //                    // enqueue the inputChannel at the end to avoid starvation
//    //                    singleInputGate.queueChannelUnsafe(
//    //                            inputChannel, bufferAndAvailability.morePriorityEvents());
//    //                }
//    //                singleInputGate.checkUnavailability();
//    //                return Optional.of(
//    //                        new InputGate.InputWithData<>(
//    //                                inputChannel,
//    //                                bufferAndAvailability,
//    //                                !inputChannelsWithData.isEmpty(),
//    //                                false));
//    //            }
//    //            // If there is no data in fetcherDataQueue
//    //            else {
//    //                Optional<InputChannel> inputChannelOpt = singleInputGate.getChannel(blocking);
//    //                if (inputChannelOpt.isPresent()) {
//    //                    inputChannel = inputChannelOpt.get();
//    //                    bufferAndAvailabilityOpt = inputChannel.getNextBuffer();
//    //                    if (!bufferAndAvailabilityOpt.isPresent()) {
//    //                         singleInputGate.checkUnavailability();
//    //                    } else {
//    //                        final InputChannel.BufferAndAvailability bufferAndAvailability =
//    //                                bufferAndAvailabilityOpt.get();
//    //                        if (bufferAndAvailability.moreAvailable()) {
//    //                            // enqueue the inputChannel at the end to avoid starvation
//    //                            singleInputGate.queueChannelUnsafe(
//    //                                    inputChannel, bufferAndAvailability.morePriorityEvents());
//    //                        }
//    //                        // singleInputGate.checkUnavailability();
//    //                        Buffer buffer = bufferAndAvailability.buffer();
//    //                        if (buffer.getDataType() == Buffer.DataType.SEGMENT_EVENT) {
//    //                            // Deserialize the EndOfSegmentEvent
//    //                            EndOfSegmentEvent endOfSegmentEvent =
//    //                                    (EndOfSegmentEvent)
//    //                                            EventSerializer.fromSerializedEvent(
//    //                                                    buffer.getNioBufferReadable(),
//    //                                                    Thread.currentThread()
//    //                                                            .getContextClassLoader());
//    //                            long targetSegmentId = endOfSegmentEvent.getSegmentId();
//    //                            while (true) {
//    //                                // Ask the upstream the which tier contain current segment
//    //                                int tierContainingSegment =
//    //                                        findTierContainingSegment(
//    //                                                inputChannel, targetSegmentId);
//    //                                if (tierContainingSegment == 1 || tierContainingSegment == 2)
//    // {
//    //                                    inputChannelConsumedSegmentIds.put(
//    //                                            inputChannel, targetSegmentId - 1);
//    //                                    break;
//    //                                }
//    //                                if (fetcherDataQueue.isSegmentExist(
//    //                                        endOfSegmentEvent.getSegmentId(), inputChannel)) {
//    //                                    if (fetcherDataQueue.getState()
//    //                                            != FetcherDataQueueState.WAITING) {
//    //                                        throw new IOException(
//    //                                                "Trying to set segment info when data fetcher
//    // is still running.");
//    //                                    }
//    //                                    inputChannelConsumedSegmentIds.put(
//    //                                            inputChannel, targetSegmentId - 1);
//    //                                    fetcherDataQueue.setSegmentInfo(
//    //                                            inputChannel, endOfSegmentEvent.getSegmentId());
//    //                                    break;
//    //                                }
//    //                            }
//    //                        } else {
//    //                            return Optional.of(
//    //                                    new InputGate.InputWithData<>(
//    //                                            inputChannel,
//    //                                            bufferAndAvailability,
//    //                                            !inputChannelsWithData.isEmpty(),
//    //                                            false));
//    //                        }
//    //                    }
//    //                } else {
//    //                    for (InputChannel singleInputChannel : allInputChannels) {
//    //                        long nextConsumedSegmentId =
//    //                                inputChannelConsumedSegmentIds.getOrDefault(
//    //                                                singleInputChannel, -1L)
//    //                                        + 1;
//    //                        LOG.info("### fetcherDataQueue is asking whether the segment {}
//    // exists.", nextConsumedSegmentId);
//    //                        if (fetcherDataQueue.isSegmentExist(
//    //                                nextConsumedSegmentId, singleInputChannel)) {
//    //                            LOG.info("### EXISTED.");
//    //                            if (fetcherDataQueue.getState() != FetcherDataQueueState.WAITING)
//    // {
//    //                                throw new IOException(
//    //                                        "Trying to set segment info when data fetcher is still
//    // running.");
//    //                            }
//    //                            inputChannelConsumedSegmentIds.put(
//    //                                    singleInputChannel, nextConsumedSegmentId);
//    //                            fetcherDataQueue.setSegmentInfo(
//    //                                    singleInputChannel, nextConsumedSegmentId);
//    //                            break;
//    //                        }
//    //                    }
//    //                }
//    //            }
//    //        }
//    //    }
//    // }
//
//    private int findTierContainingSegment(InputChannel inputChannel, long segmentId) {
//        while (true) {
//            int tier;
//            try {
//                SegmentIdAndTier segmentIdAndTier = inputChannel.containSegment(segmentId).get();
//                checkState(
//                        segmentIdAndTier.getSegmentId() == segmentId,
//                        "Get wrong containSegmentResponse from upstream!");
//                tier = segmentIdAndTier.getTier();
//                if (tier != -1) {
//                    LOG.debug(
//                            "%%% findTierContainingSegment1 segmentId {} tier {}", segmentId, tier);
//                    if (tier == 1) {
//                        checkState(fetcherDataQueue.isSegmentExist(segmentId, inputChannel));
//                    }
//                    return tier;
//                }
//            } catch (InterruptedException | ExecutionException e) {
//                throw new RuntimeException("Failed to findTierContainingSegment!");
//            }
//            LOG.info("%%% findTierContainingSegment segmentId {} tier {}", segmentId, tier);
//        }
//    }
//
//    private int findTierContainingSegmentOneTime(InputChannel inputChannel, long segmentId) {
//        int tier;
//        try {
//            SegmentIdAndTier segmentIdAndTier = inputChannel.containSegment(segmentId).get();
//            checkState(
//                    segmentIdAndTier.getSegmentId() == segmentId,
//                    "Get wrong containSegmentResponse from upstream!");
//            tier = segmentIdAndTier.getTier();
//        } catch (InterruptedException | ExecutionException e) {
//            throw new RuntimeException("Failed to findTierContainingSegmentOneTime!");
//        }
//        if (tier == 1) {
//            checkState(fetcherDataQueue.isSegmentExist(segmentId, inputChannel));
//        }
//        LOG.info("%%% findTierContainingSegmentOneTime segmentId {} tier {}", segmentId, tier);
//        return tier;
//    }
//
//    @Override
//    public Optional<InputGate.InputWithData<InputChannel, InputChannel.BufferAndAvailability>>
//            waitAndGetNextData(boolean blocking) throws IOException, InterruptedException {
//        while (true) {
//            Optional<InputChannel.BufferAndAvailability> bufferAndAvailabilityOpt;
//            InputChannel inputChannel;
//            // If there is data in fetcherDataQueue, get data from it
//            if (fetcherDataQueue.getState() == FetcherDataQueueState.RUNNING) {
//                bufferAndAvailabilityOpt = fetcherDataQueue.getNextBuffer(true);
//                inputChannel = checkNotNull(fetcherDataQueue.getCurrentChannel().orElse(null));
//                if (!bufferAndAvailabilityOpt.isPresent()) {
//                    fetcherDataQueue.setState(FetcherDataQueueState.WAITING);
//                    inputChannelConsumedSegmentIds[inputChannel.getChannelIndex()]++;
//                    // Ask the upstream the which tier contain current segment
//                    long nextSegmentId = fetcherDataQueue.getCurrentSegmentId(inputChannel) + 1;
//                    int tierContainingSegment =
//                            findTierContainingSegmentOneTime(inputChannel, nextSegmentId);
//                    checkState(
//                            (inputChannelConsumedSegmentIds[inputChannel.getChannelIndex()] + 1)
//                                    == nextSegmentId,
//                            "SegmentID is illegal.");
//                    // 1. If the tier is not in LOCAL/DFS:
//                    if (tierContainingSegment == -1) {
//                        // set a flag indicating the current input channel has just consumed from
//                        // dfs
//                        consumedFromDfs[inputChannel.getChannelIndex()] = true;
//                        synchronized (inputChannelsWithData) {
//                            singleInputGate.queueChannelUnsafe(inputChannel, false);
//                        }
//                    }
//                    // 2. If the tier is in LOCAL:
//                    else if (tierContainingSegment == 0) {
//                    }
//                    // 3. If the tier is in DFS:
//                    else if (tierContainingSegment == 1) {
//                        if (fetcherDataQueue.getState() != FetcherDataQueueState.WAITING) {
//                            throw new IOException(
//                                    "Trying to set segment info when data fetcher is still running.");
//                        }
//                        fetcherDataQueue.setSegmentInfo(inputChannel, nextSegmentId);
//                    } else {
//                        throw new RuntimeException("The tier is illegal!");
//                    }
//                } else {
//                    final InputChannel.BufferAndAvailability bufferAndAvailability =
//                            bufferAndAvailabilityOpt.get();
//                    synchronized (inputChannelsWithData) {
//                        if (bufferAndAvailability.moreAvailable()) {
//                            // enqueue the inputChannel at the end to avoid starvation
//                            singleInputGate.queueChannelUnsafe(
//                                    inputChannel, bufferAndAvailability.morePriorityEvents());
//                        }
//                        singleInputGate.checkUnavailability();
//                        return Optional.of(
//                                new InputGate.InputWithData<>(
//                                        inputChannel,
//                                        bufferAndAvailability,
//                                        !inputChannelsWithData.isEmpty(),
//                                        false));
//                    }
//                }
//            }
//            // If there is no data in fetcherDataQueue
//            else {
//                synchronized (inputChannelsWithData) {
//                    Optional<InputChannel> inputChannelOpt = singleInputGate.getChannel(blocking);
//                    if (!inputChannelOpt.isPresent()) {
//                        return Optional.empty();
//                    }
//                    inputChannel = inputChannelOpt.get();
//                    bufferAndAvailabilityOpt = inputChannel.getNextBuffer();
//                    if (!bufferAndAvailabilityOpt.isPresent()) {
//                        if (consumedFromDfs[inputChannel.getChannelIndex()]){
//                            long nextSegmentId = inputChannelConsumedSegmentIds[inputChannel.getChannelIndex()]
//                                    + 1L;
//                            int tierContainingSegment =
//                                    findTierContainingSegmentOneTime(inputChannel, nextSegmentId);
//                            if(tierContainingSegment == -1){
//                                singleInputGate.queueChannelUnsafe(inputChannel, false);
//                            }else if(tierContainingSegment == 0){
//                                consumedFromDfs[inputChannel.getChannelIndex()] = false;
//                                singleInputGate.queueChannelUnsafe(inputChannel, false);
//                            }else if(tierContainingSegment == 1){
//                                consumedFromDfs[inputChannel.getChannelIndex()] = false;
//                                if (fetcherDataQueue.getState() != FetcherDataQueueState.WAITING) {
//                                    throw new IOException(
//                                            "Trying to set segment info when data fetcher is still running.");
//                                }
//                                fetcherDataQueue.setSegmentInfo(inputChannel, nextSegmentId);
//                            }else{
//                                throw new RuntimeException("The tier is illegal!");
//                            }
//                        }
//
//                        singleInputGate.checkUnavailability();
//                        continue;
//                    }
//                }
//                final InputChannel.BufferAndAvailability bufferAndAvailability =
//                        bufferAndAvailabilityOpt.get();
//                synchronized (inputChannelsWithData) {
//                    if (bufferAndAvailability.moreAvailable()) {
//                        // enqueue the inputChannel at the end to avoid starvation
//                        singleInputGate.queueChannelUnsafe(
//                                inputChannel, bufferAndAvailability.morePriorityEvents());
//                    }
//                    singleInputGate.checkUnavailability();
//                }
//                Buffer buffer = bufferAndAvailability.buffer();
//                if (buffer.getDataType() == Buffer.DataType.SEGMENT_EVENT) {
//                    // Recording the latest consumed segment for current input channel
//                    inputChannelConsumedSegmentIds[inputChannel.getChannelIndex()]++;
//                    // Deserialize the EndOfSegmentEvent
//                    EndOfSegmentEvent endOfSegmentEvent =
//                            (EndOfSegmentEvent)
//                                    EventSerializer.fromSerializedEvent(
//                                            buffer.getNioBufferReadable(),
//                                            Thread.currentThread().getContextClassLoader());
//                    // Ask the upstream the which tier contain current segment
//                    int tierContainingSegment =
//                            findTierContainingSegment(
//                                    inputChannel, endOfSegmentEvent.getSegmentId());
//                    if (tierContainingSegment != 1) {
//                        continue;
//                    }
//                    if (fetcherDataQueue.getState() != FetcherDataQueueState.WAITING) {
//                        throw new IOException(
//                                "Trying to set segment info when data fetcher is still running.");
//                    }
//                    fetcherDataQueue.setSegmentInfo(inputChannel, endOfSegmentEvent.getSegmentId());
//                    continue;
//                }
//                synchronized (inputChannelsWithData) {
//                    return Optional.of(
//                            new InputGate.InputWithData<>(
//                                    inputChannel,
//                                    bufferAndAvailability,
//                                    !inputChannelsWithData.isEmpty(),
//                                    false));
//                }
//            }
//        }
//    }
//
//    @Override
//    public void close() throws IOException {
//        fetcherDataQueue.close();
//    }
//}
