package StreamPartitioning.parts;

import StreamPartitioning.aggregators.PartitionReportingAggregator.PartitionReportingAggregator;
import StreamPartitioning.types.Edge;
import StreamPartitioning.types.UserQuery;
import StreamPartitioning.types.Vertex;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.match.MatchBinder;

import java.util.concurrent.CompletableFuture;

/**
 * Simple Stores the message in the available storage back-end
 */
public class SimpleStoragePart extends BasePart{


    public SimpleStoragePart(){
        super();
    }


    @Override
    public void invoke(Context context, Object msg) {
        // 1. Check if this Part should do something. Mainly about CRUD operations
        if(msg instanceof UserQuery){

            UserQuery query = (UserQuery)msg;
            boolean isVertex = query.element instanceof Vertex;
            boolean isEdge = query.element instanceof Edge;
            if(!isVertex && !isEdge) throw new UnsupportedOperationException("Input Stream Element can be of type (Vertex | Edge)");

            switch (query.op){
                case ADD -> {
                    if (isVertex) getStorage().addVertex((Vertex) query.element);
                    else getStorage().addEdge((Edge) query.element);
                }
                case UPDATE -> {
                    if (isVertex) getStorage().updateVertex((Vertex) query.element);
                    else getStorage().updateEdge((Edge) query.element);
                }
                case REMOVE -> System.out.println("Remove Operation");
                default -> System.out.println("Undefined Operation");
            }
        }
        // Check if there is any aggregator responsible for this guy
        // If there is call its dispatch method
        aggFunctions.forEach((fn)->{
            if(fn.shouldTrigger(msg))fn.dispatch(context,msg);
        });

    }
}
