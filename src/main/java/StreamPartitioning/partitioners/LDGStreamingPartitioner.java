package StreamPartitioning.partitioners;

import StreamPartitioning.types.Edge;
import StreamPartitioning.types.Identifiers;
import StreamPartitioning.types.UserQuery;
import StreamPartitioning.types.Vertex;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.match.MatchBinder;

import java.util.HashMap;


/** LDG Paper based, Edge-cut, Streaming, Graph Partitioning Aglorithm.
 * @param table Stores Verex:=>Array map where array is of size NUM_PARTS+1, storing the in-neighborhood of vertex at that part + the assigned part id
 */
public class LDGStreamingPartitioner extends BasePartitioner{
    public HashMap<Vertex,int[]> table;
    public int capacity = 300; // Can be stored as an array as well
    public int[] filled;

    public LDGStreamingPartitioner(){
        super();
        table = new HashMap<>();
        filled = new int[NUM_PARTS];
    }

    @Override
    public LDGStreamingPartitioner setNUM_PARTS(short NUM_PARTS) {
        super.setNUM_PARTS(NUM_PARTS);
        filled = new int[NUM_PARTS]; // Difference is that since we are using builder design we need to re-create with correct NUM_PARTS
        return this;
    }

    @Override
    public void configure(MatchBinder matchBinder) {
        matchBinder.predicate(UserQuery.class,this::partition);
    }

    /**
     * gFunction defined in the paper of LDG
     * @param partition partition over which to calculate
     * @param v vertex over which to calculate
     * @return double score function(Higher the better)
     */
    public double gFunction(int partition,Vertex v){
        if(!table.containsKey(v))return 0.0; // This vertex has no edges yet
        return table.get(v)[partition] * (1 - (filled[partition]/capacity));
    }

    /**
     * Initializes single entry in the Hash-Map for one vertex
     * @return
     */
    public int[] initializeTableRow(){
        int [] tmp = new int[NUM_PARTS+1];
        tmp[NUM_PARTS] = -1;// Unassigned Partition
        return tmp;
    }


    public int vertexAddition(Context c, Vertex v,UserQuery q){
        // 1. Select Best Partition
        double maxValue = -1;
        int selectedPart = -1;
        for(int i=0;i<NUM_PARTS;i++){
            double value = this.gFunction(i,v);
            if(value > maxValue){
                selectedPart = i;
                maxValue = value;
            }
        }

        //2. Update Filled number in selected Partition
        filled[selectedPart]++;
        // 3. Add the selected partition to the last index of hashmap
        table.putIfAbsent(v,initializeTableRow());
        table.get(v)[NUM_PARTS] = selectedPart;
        // 4. Send Message & Return Value
        c.send(Identifiers.PART_TYPE,String.valueOf(selectedPart),q);
        return selectedPart;
    }

    public int edgeAddition(Context c,Edge e,UserQuery q){
        // 1. Place the source vertex if it is not in part and get the sourcePart to place edge along with it
        int sourcePart;
        if(!table.containsKey(e.source) || table.get(e.source)[NUM_PARTS]==-1){
            sourcePart = vertexAddition(c,e.source,new UserQuery(e.source).changeOperation(UserQuery.OPERATORS.ADD)); // Place Vertex First
        }
        else{
            sourcePart = table.get(e.source)[NUM_PARTS];
        }

        // 2. Increment the in-neighbor table of destination vertex with source part index
        table.putIfAbsent(e.destination,initializeTableRow());
        table.get(e.destination)[sourcePart]++;
        // 3. Place the destination vertex if it is not in part. NOTE: No empty checking required for destination vertex
        if(table.get(e.destination)[NUM_PARTS]==-1){
            vertexAddition(c,e.destination,new UserQuery(e.destination).changeOperation(UserQuery.OPERATORS.ADD)); // Place Vertex First
        }
        // 4. Place the Edge along with Source Vertex Partition
        c.send(Identifiers.PART_TYPE,String.valueOf(sourcePart),q);
        return sourcePart;

    }

    /**
     * Entrypoint for all UserQuery typed messages
     * @param c Context defined by libary
     * @param query UserQuery object
     */
    public void partition(Context c, UserQuery query){
        boolean isVertex = query.element instanceof Vertex;
        boolean isEdge = query.element instanceof Edge;
        if(!isVertex && !isEdge) throw new UnsupportedOperationException("Input Stream Element can be of type (Vertex | Edge)");

        switch (query.op){
            case ADD -> {
                if(isVertex){
                    vertexAddition(c,(Vertex) query.element,query);
                }else{
                    edgeAddition(c,(Edge) query.element,query);
                }
            }
            default -> {
                System.out.println("Undefined Operation");
            }
        }

    }


}
