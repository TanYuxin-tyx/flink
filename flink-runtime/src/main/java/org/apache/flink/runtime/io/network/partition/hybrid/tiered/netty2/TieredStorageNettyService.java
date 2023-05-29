package org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty2;

/**
 * {@link TieredStorageNettyService} is used to create netty services in producer and consumer side.
 */
public class TieredStorageNettyService {
    /**
     * Create the netty service in producer side.
     *
     * @return the producer netty service.
     */
    public ProducerNettyService createProducerNettyService() {
        return new ProducerNettyServiceImpl();
    }

    /**
     * Create the netty service in consumer side.
     *
     * @return the consumer netty service.
     */
    public ConsumerNettyService createConsumerNettyService() {
        return new ConsumerNettyServiceImpl();
    }
}
