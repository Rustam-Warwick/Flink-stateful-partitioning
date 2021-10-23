package StreamPartitioning.parts;

import StreamPartitioning.types.Edge;
import StreamPartitioning.types.UserQuery;
import StreamPartitioning.types.Vertex;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.match.MatchBinder;

/**
 * Simple Stores the message in the available storage back-end
 */
public class SimpleStoragePart extends BasePart{


    public SimpleStoragePart(){
        super();
    }

    @Override
    public void configure(MatchBinder matchBinder) {
        matchBinder.predicate(UserQuery.class,this::dispatch);
    }

    @Override
    public void dispatch(Context c, UserQuery query) {
        boolean isVertex = query.element instanceof Vertex;
        boolean isEdge = query.element instanceof Edge;
        if(!isVertex && !isEdge) throw new UnsupportedOperationException("Input Stream Element can be of type (Vertex | Edge)");

        switch (query.op){
            case ADD -> {
                if (isVertex) getStorage().addVertex((Vertex) query.element);
                else getStorage().addEdge((Edge) query.element);
            }
            case UPDATE -> System.out.println("Update Operation");
            case REMOVE -> System.out.println("Remove Operation");
            default -> System.out.println("Undefined Operation");
        }
    }
}
