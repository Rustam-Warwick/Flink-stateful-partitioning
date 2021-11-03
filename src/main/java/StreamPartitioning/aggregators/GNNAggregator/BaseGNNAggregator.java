package StreamPartitioning.aggregators.GNNAggregator;

import StreamPartitioning.aggregators.BaseAggregator;
import StreamPartitioning.types.Edge;
import StreamPartitioning.types.UserQuery;
import StreamPartitioning.vertex.BaseReplicatedVertex;
import org.apache.flink.shaded.guava18.com.google.common.collect.BoundType;
import org.apache.flink.statefun.sdk.Context;

import java.util.ArrayList;
import java.util.concurrent.Future;

public class BaseGNNAggregator extends BaseAggregator {
    @Override
    public boolean shouldTrigger(Object e){
        return true;
    }
    public void startGNNCell(Short l, BaseReplicatedVertex vertex,Context c){
        vertex.getOutParts().whenComplete((list,exception)->{
            list.forEach(item->{

            });
        });





    }

    public void handleUserQuery(Context ctx, UserQuery query){
        boolean isVertex = query.element instanceof BaseReplicatedVertex;
        boolean isEdge = query.element instanceof Edge;
        if(!isVertex && !isEdge)return;
        switch (query.op){
            case ADD -> {
                if(isEdge){
                    Edge tmp = (Edge) query.element;
                    this.startGNNCell((short) 1,this.part.getStorage().getVertex(tmp.destination.getId()),ctx);
                }
            }
        }

    }
    @Override
    public void dispatch(Context ctx, Object msg) {
        // Simply type check -> Cast -> Dispatch to corresponding functions
        if(msg instanceof UserQuery)handleUserQuery(ctx,(UserQuery) msg);
//        if(msg instanceof GNNQuery)handleGNNQuery(ctx,(GNNQuery) msg);
    }
}
