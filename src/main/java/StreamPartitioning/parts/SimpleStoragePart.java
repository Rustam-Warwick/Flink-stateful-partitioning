package StreamPartitioning.parts;

import StreamPartitioning.features.Feature;
import StreamPartitioning.types.Edge;
import StreamPartitioning.types.GraphQuery;
import StreamPartitioning.vertex.BaseReplicatedVertex;
import org.apache.flink.statefun.sdk.Context;

/**
 * Simple Stores the message in the available storage back-end. Only works for vertex-cut operations
 */
public class SimpleStoragePart<VT extends BaseReplicatedVertex> extends BasePart{


    public SimpleStoragePart(){
        super();
    }


    @Override
    public void invoke(Context context, Object msg) {
        // 1. Check if this Part should do something. Mainly about CRUD operations

        if(msg instanceof GraphQuery){
            GraphQuery query = (GraphQuery)msg;
            boolean isVertex = query.element instanceof BaseReplicatedVertex;
            boolean isEdge = query.element instanceof Edge;
            boolean isFeature = query.element instanceof Feature;
            if(!isVertex && !isEdge && !isFeature) throw new UnsupportedOperationException("Input Stream Element can be of type (Vertex | Edge | Feature)");
            switch (query.op){
                case ADD -> {
                    if (isEdge){
                        Edge tmp  = (Edge) query.element;
                        getStorage().addEdge(tmp,context);
                    }
                }
                case REMOVE -> System.out.println("Remove Operation");
                case SYNC -> {
                   if(isFeature){
                       Feature tmp = (Feature) query.element;
                       System.out.println(tmp);
                   }
                }
                default -> System.out.println("Undefined Operation");
            }
        }
        if(msg instanceof Feature){
            System.out.println("Here");
        }
        // Check if there is any aggregator responsible for this guy
        // If there is call its dispatch method
        aggFunctions.forEach((fn)->{
            if(fn.shouldTrigger(msg))fn.dispatch(context,msg);
        });

    }
}
