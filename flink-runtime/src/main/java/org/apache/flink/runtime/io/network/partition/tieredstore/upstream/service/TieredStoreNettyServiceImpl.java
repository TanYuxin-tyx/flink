package org.apache.flink.runtime.io.network.partition.tieredstore.upstream.service;

import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.NettyBasedTierConsumerViewProvider;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierReaderView;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierWriter;

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
        List<TierWriter> registeredTiers = new ArrayList<>();
        List<TierReaderView> registeredTierReaderViews = new ArrayList<>();
        for (TierWriter tierWriter : tierWriters) {
            if (tierWriter instanceof NettyBasedTierConsumerViewProvider) {
                NettyBasedTierConsumerViewProvider nettyBasedTierConsumerViewProvider =
                        (NettyBasedTierConsumerViewProvider) tierWriter;
                TierReaderView tierReaderView =
                        checkNotNull(
                                nettyBasedTierConsumerViewProvider.createTierReaderView(
                                        subpartitionId, availabilityListener));
                registeredTiers.add(tierWriter);
                registeredTierReaderViews.add(tierReaderView);
            }
        }
        return new TieredStoreResultSubpartitionView(
                subpartitionId, availabilityListener, registeredTiers, registeredTierReaderViews);
    }
}
