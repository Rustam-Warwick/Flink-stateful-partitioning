package StreamPartitioning.parts;

import StreamPartitioning.types.Edge;
import StreamPartitioning.types.UserQuery;
import StreamPartitioning.types.QueryOperation;
import StreamPartitioning.types.Vertex;
import org.apache.flink.statefun.sdk.Context;

/**
 * Simple Stores the message in the available storage back-end
 */
public class SimpleStoragePart extends IncrementalPart {


    SimpleStoragePart(){
        super();
    }

    @Override
    public void invoke(Context context, Object o) {
        UserQuery input = (UserQuery) o;
        if(input.op== QueryOperation.ADD){
            if(input.element instanceof Vertex){
                // Vertex op
            }
            else if(input.element instanceof Edge){
                // Edge operation
                Edge edge = (Edge) input.element;
                storage.addEdge(edge);

            }
        }
    }
}
