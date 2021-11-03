package StreamPartitioning.parts;

import StreamPartitioning.types.Edge;
import StreamPartitioning.types.UserQuery;
import StreamPartitioning.vertex.BaseReplicatedVertex;
import org.apache.flink.statefun.sdk.Context;

/**
 * Simple Stores the message in the available storage back-end. Only works for vertex-cut operations
 */
public class SimpleStoragePart extends BasePart{


    public SimpleStoragePart(){
        super();
    }


    @Override
    public void invoke(Context context, Object msg) {
        // 1. Check if this Part should do something. Mainly about CRUD operations
        System.out.println("Message Arrived");
        if(msg instanceof UserQuery){

            UserQuery query = (UserQuery)msg;
            boolean isVertex = query.element instanceof BaseReplicatedVertex;
            boolean isEdge = query.element instanceof Edge;
            if(!isVertex && !isEdge) throw new UnsupportedOperationException("Input Stream Element can be of type (Vertex | Edge)");
            switch (query.op){
                case ADD -> {
                    if (isEdge){
                        Edge tmp  = (Edge) query.element;
                        getStorage().addEdge(tmp);
                        getStorage().getVertex(tmp.source.getId()).updateMaster(context);
                        getStorage().getVertex(tmp.destination.getId()).updateMaster(context);
                    }
                }
                case UPDATE -> {
                    if (isVertex) {
                        BaseReplicatedVertex incoming = (BaseReplicatedVertex) query.element;
                        BaseReplicatedVertex replica = getStorage().getVertex(incoming.getId());
                        if(replica!=null)replica.sync(context,incoming);
                    }
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
