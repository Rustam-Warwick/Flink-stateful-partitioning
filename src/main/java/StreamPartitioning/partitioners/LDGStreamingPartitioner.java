package StreamPartitioning.partitioners;


/**
 * LDG Paper based, Edge-cut, Streaming, Graph Partitioning Aglorithm.
 * Edges can arrive before vertices hence the underlying storage mechanism should support that.
 * Slight modification. Idea: Place incoming vertices to partitioning where there are more incoming edges.
 * Part numbers start from 0
 *  */
//public class LDGStreamingPartitioner extends BasePartitioner{
//    public HashMap<String,int[]> table;
//    public int capacity = 300; // Can be stored as an array as well
//    public int[] filled;
//
//    public LDGStreamingPartitioner(){
//        super();
//        table = new HashMap<>();
//        filled = new int[NUM_PARTS];
//    }
//
//    @Override
//    public LDGStreamingPartitioner setNUM_PARTS(short NUM_PARTS) {
//        super.setNUM_PARTS(NUM_PARTS);
//        filled = new int[NUM_PARTS]; // Difference is that since we are using builder design we need to re-create with correct NUM_PARTS
//        return this;
//    }
//
//    @Override
//    public void configure(MatchBinder matchBinder) {
//        matchBinder.predicate(UserQuery.class,this::partition);
//    }
//
//    /**
//     * gFunction defined in the paper of LDG
//     * @param partition partition over which to calculate
//     * @param v vertex over which to calculate
//     * @return double score function(Higher the better)
//     */
//    public double gFunction(int partition,Vertex v){
//        if(!table.containsKey(v.getId()))return 0.0; // This vertex has no edges yet
//        return table.get(v.getId())[partition] * (1 - (filled[partition]/capacity));
//    }
//
//    /**
//     * Initializes single entry in the Hash-Map for one vertex
//     * @return
//     */
//    public int[] initializeTableRow(){
//        int [] tmp = new int[NUM_PARTS+1];
//        tmp[NUM_PARTS] = -1;// Unassigned Partition
//        return tmp;
//    }
//
//    /**
//     * If assigned returns the part id, else returns best part id according to gfunction
//     * @param v Vertex
//     * @return int Part Id
//     */
//    public int vertexAddition(Vertex v){
//        // 1. Select Best Partition if not already there
//        table.putIfAbsent(v.getId(),initializeTableRow());
//        int selectedPart = table.get(v.getId())[NUM_PARTS];
//
//        if(selectedPart==-1){
//            // 2. Assign a new Part
//            double maxScore = -1;
//            short numCandidates = 0; // Num of candidate parts with same g-score
//            double scores[] = new double[NUM_PARTS];
//            // 2.2 Get all the scores for partitions
//            for(int i=0;i<NUM_PARTS;i++){
//                double value = this.gFunction(i,v);
//                scores[i] = value;
//                if(value > maxScore){
//                    numCandidates = 1;
//                    maxScore = value;
//                }
//                else if(value==maxScore){
//                    numCandidates++;
//                }
//            }
//            // 2.3. Select a random partition amongst the candidate ones
//            int selectedCandidate = (int) (Math.random()*numCandidates);
//            int tmp=0;
//            for(int i=0;i<NUM_PARTS;i++){
//                if(scores[i]==maxScore){
//                    if(tmp==selectedCandidate){
//                        selectedPart = i;
//                        break;
//                    }
//                    tmp++;
//                }
//            }
//
//            //3. Update Filled number in selected Partition
//            filled[selectedPart]++;
//            table.get(v.getId())[NUM_PARTS] = selectedPart;
//        }
//        return selectedPart;
//    }
//    public int edgeAddition(Edge e){
//
//        // 1. Check if source is partitioned. If no assign to a partition
//        table.putIfAbsent(e.source._1,initializeTableRow());
//        Integer sourcePart;
//        Integer destPart = null;
//        if(table.get(e.source._1)[NUM_PARTS]==-1) {
//            // 1.1. Source not partitionined
//            sourcePart = vertexAddition(new Vertex().withId(e.source._1));
//            table.get(e.source._1)[NUM_PARTS] = sourcePart; // Insert to source part
//        }else sourcePart = table.get(e.source._1)[NUM_PARTS];
//
//
//        // 2. Increment Destination
//        table.putIfAbsent(e.destination._1,initializeTableRow());
//        table.get(e.destination._1)[sourcePart]++;
//
//        // 3. Get destPart if it is avaialable
//        if(table.get(e.destination._1)[NUM_PARTS]!=-1){
//            destPart = table.get(e.destination._1)[NUM_PARTS];
//        }
//
//        // 4. Add source part and dest part to the edge source and dest
//        e.betweenVertices(e.source.copy(e.source._1,sourcePart),e.destination.copy(e.destination._1,destPart));
//        // 5. Return Edge partition and increment filled
//        filled[sourcePart]++;
//        return sourcePart;
//    }
//
//
//    /**
//     * Entrypoint for all UserQuery typed messages
//     * @param c Context defined by libary
//     * @param query UserQuery object
//     */
//    public void partition(Context c, UserQuery query){
//        boolean isVertex = query.element instanceof Vertex;
//        boolean isEdge = query.element instanceof Edge;
//        if(!isVertex && !isEdge) throw new UnsupportedOperationException("Input Stream Element can be of type (Vertex | Edge)");
//        try {
//            switch (query.op) {
//                case ADD -> {
//                    if (isVertex) {
//                        // 1. Assign new Vertex to a part and send
//                        Vertex newVertex = (Vertex) query.element;
////                        System.out.format("Incoming Vertex %s\n",newVertex.getId());
//                        int selectedPart = vertexAddition(newVertex);
//                        c.send(Identifiers.PART_TYPE,String.valueOf(selectedPart),query);
//                        // 2. Update its in-neighbors in different parts such that they know each other
//                        Vertex tmpVertex = new Vertex().withId(newVertex.getId()).inPart(selectedPart);
//                        UserQuery tmpQuery = new UserQuery(tmpVertex).changeOperation(UserQuery.OPERATORS.UPDATE);
//                        for(int i=0;i<NUM_PARTS;i++){
//                            if(i==selectedPart)continue;
//                            if(table.get(newVertex.getId())[i]>0)c.send(Identifiers.PART_TYPE,String.valueOf(i),tmpQuery);
//                        }
//
//                    } else {
//                        // 1. Assign Edge to a part.
//                        Edge newEdge = (Edge) query.element;
////                        System.out.format("Incoming Edge %s\n",newEdge.getId());
//                        int selectedPart = edgeAddition(newEdge);
//                        c.send(Identifiers.PART_TYPE,String.valueOf(selectedPart),query);
//                    }
//                }
//                default -> {
//                    System.out.println("Undefined Operation");
//                }
//            }
//        }catch (Exception e){
//            System.out.println(e);
//        }
//
//    }
//
//
//}
