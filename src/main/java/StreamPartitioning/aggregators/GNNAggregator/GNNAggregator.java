package StreamPartitioning.aggregators.GNNAggregator;

import StreamPartitioning.aggregators.BaseAggregator;
import StreamPartitioning.parts.BasePart;
import StreamPartitioning.types.Edge;
import StreamPartitioning.types.UserQuery;
import StreamPartitioning.types.Vertex;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;

/**
 * Flexible GNN Aggregation function.
 *
 */
public class GNNAggregator extends BaseAggregator {
    public Class[] acceptedTypes = new Class[]{UserQuery.class};

    public int L = 2;

    public GNNAggregator withNeighborhood(int L){
        L=L;
        return this;
    }




    @Override
    public boolean shouldTrigger(Object e) {
        Class originalClass = e.getClass();
        for(var i=0;i<acceptedTypes.length;i++){
            if(originalClass.equals(acceptedTypes[i]))return true;
        }
        return false;
    }


    public void handleUserQuery(Context ctx, UserQuery query){
        boolean isVertex = query.element instanceof Vertex;
        boolean isEdge = query.element instanceof Edge;
        if(!isVertex && !isEdge)return;
        switch (query.op){
            case ADD -> {

            }
            default -> {

            }
        }

    }


    @Override
    public void dispatch(Context ctx, Object msg) {
        // Simply type check -> Cast -> Dispatch to corresponding functions
        if(msg instanceof UserQuery)handleUserQuery(ctx,(UserQuery) msg);
    }
}

