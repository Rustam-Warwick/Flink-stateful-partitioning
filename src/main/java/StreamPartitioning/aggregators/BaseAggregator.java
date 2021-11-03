package StreamPartitioning.aggregators;
import StreamPartitioning.parts.BasePart;
import StreamPartitioning.types.Edge;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;

/**
 * Base Aggregaation Function. Note that, repartitiioning can also be defined as a sort of graph aggregation
 *
 *
 */
public abstract class BaseAggregator {
    public BasePart part = null;

    public BaseAggregator attachedTo(BasePart e){
        part =e;
        return this;

    }
    /**
     * Is the Ojects real type accepted in this aggregator. Does it care?
     * @param o Object to by typechecked
     * @return
     */
    public abstract boolean shouldTrigger(Object o);

    /**
     * If objects type is accepted then comes into dispatch
     * @param ctx Flink Streaming Context
     * @param msg Object instance
     */
    public abstract void dispatch(Context ctx, Object msg);
}
