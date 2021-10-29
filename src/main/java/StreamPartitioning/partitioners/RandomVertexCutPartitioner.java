package StreamPartitioning.partitioners;

import StreamPartitioning.types.Edge;
import StreamPartitioning.types.Identifiers;
import StreamPartitioning.types.UserQuery;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.match.MatchBinder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.stream.Stream;

/**
 * Vertex-cut Random parititoning
 * Randomly select a part from the vertex list
 *
 */
public class RandomVertexCutPartitioner extends BasePartitioner{
    public Random random;

    public HashMap<String, ArrayList<Short>> inParts = new HashMap<>(); // Parts where vertex has in-neighbors
    public HashMap<String, ArrayList<Short>> outParts = new HashMap<>(); // Parts where vertex has in-neighbors

    public RandomVertexCutPartitioner(){
        random = new Random();
    }

    @Override
    public void configure(MatchBinder matchBinder) {
        matchBinder.predicate(UserQuery.class,this::partition);
    }

    public void newEdge(Context c, UserQuery query,Short partId){
        Edge tmp = (Edge)query.element;
        inParts.putIfAbsent(tmp.destination.getId(),new ArrayList<>());
        outParts.putIfAbsent(tmp.source.getId(),new ArrayList<>());
        inParts.get(tmp.destination.getId()).add(partId);
        outParts.get(tmp.source.getId()).add(partId);
        // 3. Update the vertex info in the Edge
        tmp.source.outParts = outParts.get(tmp.source.getId());
        tmp.destination.inParts = inParts.get(tmp.destination.getId());
        // 4. Update the source vertex in all partitions,
        UserQuery sourceUpdate = new UserQuery(tmp.source).changeOperation(UserQuery.OPERATORS.UPDATE);
        UserQuery destUpdate = new UserQuery(tmp.destination).changeOperation(UserQuery.OPERATORS.UPDATE);
        Stream.concat(tmp.source.inParts.stream(),tmp.source.outParts.stream()).distinct().forEach(item->c.send(Identifiers.PART_TYPE,item.toString(),sourceUpdate));
        Stream.concat(tmp.destination.inParts.stream(),tmp.destination.outParts.stream()).distinct().forEach(item->c.send(Identifiers.PART_TYPE,item.toString(),destUpdate));
    }

    public void partition(Context c, UserQuery query){
        // 1. Pick a partition
        short partId = (short)random.nextInt(this.NUM_PARTS);
        // 2. Handle Edge addition logic
        if(query.element instanceof Edge){
            newEdge(c,query,partId);
        }
        c.send(Identifiers.PART_TYPE,String.valueOf(partId),query);




    }
}
