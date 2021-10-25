package StreamPartitioning.aggregators;
import org.apache.flink.statefun.sdk.Context;

/**
 * Base Aggregaation Function. Note that, repartitiioning can also be defined as a sort of graph aggregation
 *
 *
 */
public interface BaseAggregator {
    /**
     * Is the Ojects real type accepted in this aggregator. Does it care?
     * @param e Object to by typechecked
     * @return
     */
    boolean isTypeAccepted(Object e);

    /**
     * If objects type is accepted then comes into dispatch
     * @param ctx Flink Streaming Context
     * @param msg Object instance
     */
    void dispatch(Context ctx, Object msg);
}
