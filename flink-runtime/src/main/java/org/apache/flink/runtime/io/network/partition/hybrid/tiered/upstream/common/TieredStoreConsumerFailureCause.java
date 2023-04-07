package org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common;

import java.util.ArrayList;
import java.util.List;

/** The {@link TieredStoreConsumerFailureCause} is used to combine Exceptions from each Tier. */
public class TieredStoreConsumerFailureCause extends Throwable {
    private final List<Throwable> throwables;

    public TieredStoreConsumerFailureCause() {
        this.throwables = new ArrayList<>();
    }

    public boolean isEmpty() {
        return throwables.isEmpty();
    }

    public void appendException(Throwable e) {
        if (e != null) {
            throwables.add(e);
        }
    }

    @Override
    public String getMessage() {
        StringBuilder sb = new StringBuilder();
        for (Throwable t : throwables) {
            sb.append(t.getMessage()).append("\n");
        }
        return sb.toString();
    }
}
