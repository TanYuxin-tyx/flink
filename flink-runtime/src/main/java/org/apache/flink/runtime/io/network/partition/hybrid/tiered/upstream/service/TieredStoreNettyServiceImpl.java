package org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.service;

import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.NettyBasedTierConsumerView;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.NettyBasedTierConsumerViewProvider;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.TierWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The {@link TieredStoreNettyServiceImpl} is the implementation of {@link TieredStoreNettyService}.
 */
public class TieredStoreNettyServiceImpl implements TieredStoreNettyService {

    private final TierWriter[] tierWriters;

    public TieredStoreNettyServiceImpl(TierWriter[] tierWriters) {
        checkArgument(tierWriters.length > 0, "The number of StorageTier must be larger than 0.");
        this.tierWriters = tierWriters;
    }

    @Override
    public TieredStoreResultSubpartitionView register(
            int subpartitionId, BufferAvailabilityListener availabilityListener)
            throws IOException {
        List<NettyBasedTierConsumerViewProvider> registeredTiers = new ArrayList<>();
        List<NettyBasedTierConsumerView> registeredTierConsumerViews = new ArrayList<>();
        for (TierWriter tierWriter : tierWriters) {
            if (tierWriter instanceof NettyBasedTierConsumerViewProvider) {
                NettyBasedTierConsumerViewProvider tierConsumerViewProvider =
                        (NettyBasedTierConsumerViewProvider) tierWriter;
                NettyBasedTierConsumerView nettyBasedTierConsumerView =
                        checkNotNull(
                                tierConsumerViewProvider.createTierReaderView(
                                        subpartitionId, availabilityListener));
                registeredTiers.add(tierConsumerViewProvider);
                registeredTierConsumerViews.add(nettyBasedTierConsumerView);
            }
        }
        return new TieredStoreResultSubpartitionView(
                subpartitionId, availabilityListener, registeredTiers, registeredTierConsumerViews);
    }
}
