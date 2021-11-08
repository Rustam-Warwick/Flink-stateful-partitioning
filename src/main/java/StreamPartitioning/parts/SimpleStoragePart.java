package StreamPartitioning.parts;

import StreamPartitioning.features.Feature;
import StreamPartitioning.edges.Edge;
import StreamPartitioning.features.ReplicableFeature;
import StreamPartitioning.types.GraphQuery;
import StreamPartitioning.vertex.BaseReplicatedVertex;
import StreamPartitioning.vertex.BaseVertex;
import org.apache.flink.statefun.sdk.Context;

import java.util.ArrayList;
import java.util.logging.Logger;

/**
 * Simple Stores the message in the available storage back-end. Only works for vertex-cut operations
 */
public class SimpleStoragePart<VT extends BaseReplicatedVertex> extends BasePart<VT>{


    public SimpleStoragePart(){
        super();
    }


    @Override
    public void invoke(Context context, Object msg) {
        try {
            if (msg instanceof GraphQuery) {
                GraphQuery query = (GraphQuery) msg;
                boolean isVertex = query.element instanceof BaseReplicatedVertex;
                boolean isEdge = query.element instanceof Edge;
                boolean isFeature = query.element instanceof Feature;
                if (!isVertex && !isEdge && !isFeature)
                    throw new UnsupportedOperationException("Input Stream Element can be of type (Vertex | Edge | Feature)");
                switch (query.op) {
                    case ADD -> {
                        if (isEdge) {
                            Edge<VT> tmp = (Edge) query.element;
                            getStorage().addEdge(tmp, context);
                        }
                    }
                    case REMOVE -> System.out.println("Remove Operation");
                    case SYNC -> {
                        if (isFeature) {
                            ReplicableFeature<ArrayList<Short>> tmp = (ReplicableFeature) query.element;
                            Class<?> clazz = Class.forName(tmp.attachedToClassName);
                            if(BaseReplicatedVertex.class.isAssignableFrom(clazz)){
                                BaseReplicatedVertex vertex = getStorage().getVertex(tmp.attachedId);
                                if(vertex==null){
                                    //StringBuilder values = new StringBuilder();
                                    // System.out.format("NOT FOUND Part:%s State:%s Value:%s toPart:%s fieldName:%s \n",tmp.part,tmp.replicationState,values,context.self().id(),tmp.fieldName);
                                    return;
                                }
                                if(vertex.getId().equals("3")){
                                    StringBuilder values = new StringBuilder();
                                    for(Short a: tmp.value) values.append(a.toString()+" ");
                                }
                                vertex.updateFeatureCallback(context,tmp);
                            }
                        }
                    }
                    default -> System.out.println("Undefined Operation");
                }
            }

            // Check if there is any aggregator responsible for this guy
            // If there is call its dispatch method
            aggFunctions.forEach((fn) -> {
//                if (fn.shouldTrigger(msg)) fn.dispatch(context, msg);
            });
        }catch (ClassNotFoundException e){
            Logger.getLogger("warnng").warning(e.getMessage());
        }
        catch (Exception e){
            System.out.println("Exception in Part");
        }
    }
}
