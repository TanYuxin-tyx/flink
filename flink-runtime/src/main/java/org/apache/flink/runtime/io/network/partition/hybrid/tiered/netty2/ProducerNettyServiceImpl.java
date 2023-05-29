package org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty2;

import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.CreditBasedShuffleView;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.CreditBasedShuffleViewImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.BufferContext;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

public class ProducerNettyServiceImpl implements ProducerNettyService {

    private final Map<Integer, Queue<BufferContext>> registeredBufferQueues = new HashMap<>();

    private final Map<Integer, Runnable> registeredReleaseNotifiers = new HashMap<>();

    @Override
    public void register(
            int subpartitionId, Queue<BufferContext> bufferQueue, Runnable serviceReleaseNotifier) {
        registeredBufferQueues.put(subpartitionId, bufferQueue);
        registeredReleaseNotifiers.put(subpartitionId, serviceReleaseNotifier);
    }

    @Override
    public CreditBasedShuffleView createCreditBasedShuffleView(
            int subpartitionId, BufferAvailabilityListener availabilityListener) {
        Queue<BufferContext> bufferQueue = registeredBufferQueues.get(subpartitionId);
        Runnable releaseNotifier = registeredReleaseNotifiers.get(subpartitionId);
        registeredBufferQueues.remove(subpartitionId);
        registeredReleaseNotifiers.remove(subpartitionId);
        return new CreditBasedShuffleViewImpl(bufferQueue, availabilityListener, releaseNotifier);
    }
}
