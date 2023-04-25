package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.service;

import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.SegmentSearcher;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TierProducerAgent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The {@link TieredStoreNettyServiceImpl} is the implementation of {@link TieredStoreNettyService}.
 */
public class TieredStoreNettyServiceImpl implements TieredStoreNettyService {

    private final List<TierProducerAgent> tierProducerAgents;

    public TieredStoreNettyServiceImpl(List<TierProducerAgent> tierProducerAgents) {
        checkArgument(
                tierProducerAgents.size() > 0, "The number of StorageTier must be larger than 0.");
        this.tierProducerAgents = tierProducerAgents;
    }

    @Override
    public TieredStoreResultSubpartitionView register(
            int subpartitionId, BufferAvailabilityListener availabilityListener)
            throws IOException {
        List<SegmentSearcher> segmentSearchers = new ArrayList<>();
        List<NettyServiceView> registeredTierConsumerViews = new ArrayList<>();
        for (TierProducerAgent tierProducerAgent : tierProducerAgents) {
            if (tierProducerAgent instanceof NettyServiceViewProvider) {
                NettyServiceViewProvider tierConsumerViewProvider =
                        (NettyServiceViewProvider) tierProducerAgent;
                NettyServiceView nettyServiceView =
                        checkNotNull(
                                tierConsumerViewProvider.createNettyBasedTierConsumerView(
                                        subpartitionId, availabilityListener));
                segmentSearchers.add((SegmentSearcher) tierProducerAgent);
                registeredTierConsumerViews.add(nettyServiceView);
            }
        }
        return new TieredStoreResultSubpartitionView(
                subpartitionId, availabilityListener, segmentSearchers, registeredTierConsumerViews);
    }
}
