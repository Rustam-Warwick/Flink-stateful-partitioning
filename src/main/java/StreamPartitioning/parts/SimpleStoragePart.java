package StreamPartitioning.parts;

import StreamPartitioning.storage.GraphStorage;
import StreamPartitioning.types.Edge;
import StreamPartitioning.types.GraphQuery;
import StreamPartitioning.types.QueryOperation;
import StreamPartitioning.types.Vertex;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.annotations.Persisted;

import java.util.ArrayList;
import java.util.HashMap;

public class SimpleStoragePart extends IncrementalPart {


    SimpleStoragePart(){
        super();
    }

    @Override
    public void invoke(Context context, Object o) {
        GraphQuery input = (GraphQuery) o;
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
