package org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.service;

import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TierStorage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The {@link TieredStoreNettyServiceImpl} is the implementation of {@link TieredStoreNettyService}.
 */
public class TieredStoreNettyServiceImpl implements TieredStoreNettyService {

    private final TierStorage[] tierStorages;

    public TieredStoreNettyServiceImpl(TierStorage[] tierStorages) {
        checkArgument(tierStorages.length > 0, "The number of StorageTier must be larger than 0.");
        this.tierStorages = tierStorages;
    }

    @Override
    public TieredStoreResultSubpartitionView register(
            int subpartitionId, BufferAvailabilityListener availabilityListener)
            throws IOException {
        List<NettyBasedTierConsumerViewProvider> registeredTiers = new ArrayList<>();
        List<NettyBasedTierConsumerView> registeredTierConsumerViews = new ArrayList<>();
        for (TierStorage tierStorage : tierStorages) {
            if (tierStorage instanceof NettyBasedTierConsumerViewProvider) {
                NettyBasedTierConsumerViewProvider tierConsumerViewProvider =
                        (NettyBasedTierConsumerViewProvider) tierStorage;
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
