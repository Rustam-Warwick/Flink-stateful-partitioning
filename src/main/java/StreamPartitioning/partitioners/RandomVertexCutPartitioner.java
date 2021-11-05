package StreamPartitioning.partitioners;

import StreamPartitioning.types.Edge;
import StreamPartitioning.types.Identifiers;
import StreamPartitioning.types.GraphQuery;
import StreamPartitioning.vertex.BaseReplicatedVertex;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.match.MatchBinder;

import java.util.HashMap;
import java.util.Random;

/**
 * Vertex-cut Random parititoning
 * Randomly select a part from the vertex list
 *
 */
public class RandomVertexCutPartitioner<VT extends BaseReplicatedVertex> extends BasePartitioner{
    public Random random;

    public HashMap<String,Short> masterVertexPart = new HashMap<>();

    public RandomVertexCutPartitioner(){
        random = new Random();
    }

    @Override
    public void configure(MatchBinder matchBinder) {
        matchBinder.predicate(GraphQuery.class,this::partition);
    }
    public void newEdge(Context c, GraphQuery query, Short partId){
        Edge newEdge = (Edge) query.element;
        // 2. Resolve the Master Vertices
        if(masterVertexPart.containsKey(newEdge.source.getId())){
            // This vertex has been placed before
            // Is the placed master part different from this one
            if(masterVertexPart.get(newEdge.source.getId())!=partId)newEdge.source.setMasterPart(masterVertexPart.get(newEdge.source.getId()));
        }else{
            // this is the master vertex it is here for the first time
            masterVertexPart.put(newEdge.source.getId(),partId);
        }
        // 3. Same as step 2
        if(masterVertexPart.containsKey(newEdge.destination.getId())){
            if(masterVertexPart.get(newEdge.destination.getId())!=partId)newEdge.destination.setMasterPart(masterVertexPart.get(newEdge.destination.getId()));
        }else{
            masterVertexPart.put(newEdge.destination.getId(),partId);
        }
    }

    public void partition(Context c, GraphQuery query){
        // 1. Pick a partition
        short partId = (short)random.nextInt(this.NUM_PARTS);
        // 2. Handle Edge addition logic
        if(query.element instanceof Edge){
            newEdge(c,query,partId);
            Edge tmp = (Edge) query.element;
            System.out.format("Sending (%s,%s) to partition:%s \n",tmp.source.getId(),tmp.destination.getId(),partId);
        }

        c.send(Identifiers.PART_TYPE,String.valueOf(partId),query);

    }
}
