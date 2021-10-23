package StreamPartitioning.partitioners;

import StreamPartitioning.types.Edge;
import StreamPartitioning.types.Identifiers;
import StreamPartitioning.types.UserQuery;
import StreamPartitioning.types.Vertex;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.match.MatchBinder;

import java.util.HashMap;

public class LDGStreamingPartitioner extends BasePartitioner{
    public HashMap<Vertex,int[]> table;
    public LDGStreamingPartitioner(){
        super();
        table = new HashMap<>();
    }

    @Override
    public void configure(MatchBinder matchBinder) {
        matchBinder.predicate(UserQuery.class,this::partition);
    }
    public void updateTable(){

    }
    public void partition(Context c, UserQuery query){
        boolean isVertex = query.element instanceof Vertex;
        boolean isEdge = query.element instanceof Edge;
        if(!isVertex && !isEdge) throw new UnsupportedOperationException("Input Stream Element can be of type (Vertex | Edge)");

        switch (query.op){
            case ADD -> {
                if(isVertex){
                    Vertex e = (Vertex) query.element;
                    table.putIfAbsent(e,new int[2]);
                }else{
                    Edge e = (Edge) query.element;
                    table.putIfAbsent(e.source,new int[2]);
                    int[] row = table.get(e.source);
                }
            }
            default -> System.out.println("Undefined Operation");
        }

    }


}
