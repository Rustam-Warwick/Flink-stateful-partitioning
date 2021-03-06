package StreamPartitioning.parts;
import StreamPartitioning.aggregators.BaseAggregator;
import StreamPartitioning.storage.GraphStorage;
import StreamPartitioning.vertex.BaseReplicatedVertex;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.java.message.Message;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;


/**
 * Base class for Part Types
 * Any part usually should have a storage but it is not a must
 * Any part usually should have a list of aggregator functions attached to it but again not must
 */
abstract public class BasePart<VT extends BaseReplicatedVertex> implements StatefulFunction {
    GraphStorage<VT> storage = null;
    ArrayList<BaseAggregator> aggFunctions = new ArrayList<>();

    public BasePart<VT> attachAggregator(BaseAggregator e){
        aggFunctions.add(e);
        e.attachedTo(this);
        return this;
    }
    public void detachAggregator(BaseAggregator e){
        aggFunctions.remove(e);
    }

    public BasePart<VT> setStorage(GraphStorage<VT> storage) {
        this.storage = storage;
        return this;
    }

    public GraphStorage<VT> getStorage() {
        if(storage==null) throw new NotImplementedException("Add a storage to graph part");
        return storage;
    }

    @Override
    abstract public void invoke(Context context, Object o);
}
