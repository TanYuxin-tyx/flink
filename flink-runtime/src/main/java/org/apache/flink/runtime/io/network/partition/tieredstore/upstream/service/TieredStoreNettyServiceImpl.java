package org.apache.flink.runtime.io.network.partition.tieredstore.upstream.service;

import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.StorageTier;
import org.apache.flink.runtime.io.network.partition.tieredstore.upstream.common.TierReaderView;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * The {@link TieredStoreNettyServiceImpl} is the implementation of {@link TieredStoreNettyService}.
 */
public class TieredStoreNettyServiceImpl implements TieredStoreNettyService {

    private final StorageTier[] allTiers;

    public TieredStoreNettyServiceImpl(StorageTier[] allTiers) {
        checkArgument(allTiers.length > 0, "The number of StorageTier must be larger than 0.");
        this.allTiers = allTiers;
    }

    @Override
    public TieredStoreResultSubpartitionView register(
            int subpartitionId, BufferAvailabilityListener availabilityListener)
            throws IOException {
        List<StorageTier> registeredTiers = new ArrayList<>();
        List<TierReaderView> registeredTierReaderViews = new ArrayList<>();
        for (StorageTier tier : allTiers) {
            TierReaderView tierReaderView =
                    tier.createTierReaderView(subpartitionId, availabilityListener);
            if (tierReaderView != null) {
                registeredTiers.add(tier);
                registeredTierReaderViews.add(tierReaderView);
            }
        }
        return new TieredStoreResultSubpartitionView(
                subpartitionId, availabilityListener, registeredTiers, registeredTierReaderViews);
    }
}