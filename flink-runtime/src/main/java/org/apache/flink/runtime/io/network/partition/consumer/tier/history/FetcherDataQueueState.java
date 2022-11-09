package org.apache.flink.runtime.io.network.partition.consumer.tier.history;

/** The state of {@link FetcherDataQueue}. */
public enum FetcherDataQueueState {
    CLOSED,
    WAITING,
    RUNNING,
    FINISHED;
}
