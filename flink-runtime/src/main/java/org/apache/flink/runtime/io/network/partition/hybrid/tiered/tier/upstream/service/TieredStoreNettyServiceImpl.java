package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.upstream.service;

import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.common.TierProducerAgent;

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
        checkArgument(tierProducerAgents.size() > 0, "The number of StorageTier must be larger than 0.");
        this.tierProducerAgents = tierProducerAgents;
    }

    @Override
    public TieredStoreResultSubpartitionView register(
            int subpartitionId, BufferAvailabilityListener availabilityListener)
            throws IOException {
        List<NettyBasedTierConsumerViewProvider> registeredTiers = new ArrayList<>();
        List<NettyBasedTierConsumerView> registeredTierConsumerViews = new ArrayList<>();
        for (TierProducerAgent tierProducerAgent : tierProducerAgents) {
            if (tierProducerAgent instanceof NettyBasedTierConsumerViewProvider) {
                NettyBasedTierConsumerViewProvider tierConsumerViewProvider =
                        (NettyBasedTierConsumerViewProvider) tierProducerAgent;
                NettyBasedTierConsumerView nettyBasedTierConsumerView =
                        checkNotNull(
                                tierConsumerViewProvider.createNettyBasedTierConsumerView(
                                        subpartitionId, availabilityListener));
                registeredTiers.add(tierConsumerViewProvider);
                registeredTierConsumerViews.add(nettyBasedTierConsumerView);
            }
        }
        return new TieredStoreResultSubpartitionView(
                subpartitionId, availabilityListener, registeredTiers, registeredTierConsumerViews);
    }
}
