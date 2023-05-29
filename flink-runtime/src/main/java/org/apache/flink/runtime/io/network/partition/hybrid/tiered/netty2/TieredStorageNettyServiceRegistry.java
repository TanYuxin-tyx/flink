package org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty2;

public class TieredStorageNettyServiceRegistry {
    ProducerNettyService registerProducerNettyService() {
        return new ProducerNettyServiceImpl();
    }

    ConsumerNettyService registerConsumerNettyService() {
        return new ConsumerNettyServiceImpl();
    }
}
