package StreamPartitioning.aggregators.GNNAggregator;

import java.util.concurrent.CompletableFuture;

public interface Aggregatable<T> {
    CompletableFuture<T> getFeature(short l);
}
