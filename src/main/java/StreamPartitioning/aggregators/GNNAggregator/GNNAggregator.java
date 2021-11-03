//package StreamPartitioning.aggregators.GNNAggregator;
//
//import StreamPartitioning.aggregators.BaseAggregator;
//import StreamPartitioning.types.Edge;
//import StreamPartitioning.types.Identifiers;
//import StreamPartitioning.types.UserQuery;
//import StreamPartitioning.vertex.BaseReplicatedVertex;
//import StreamPartitioning.vertex.Vertex;
//import org.apache.flink.statefun.sdk.Context;
//import scala.Tuple4;
//
//import java.util.*;
//
///**
// * Flexible GNN Aggregation function.
// *
// */
//public class GNNAggregator extends BaseAggregator {
//    public Class[] acceptedTypes = new Class[]{UserQuery.class,GNNQuery.class,BaseReplicatedVertex.class};
//    public HashMap<String, Tuple4<Short,Short,String,ArrayList<Integer>>> gnnQueries = new HashMap<>();
//    public short L = 2;
//
//    public GNNAggregator withNeighborhood(short L){
//        L=L;
//        return this;
//    }
//
//
//    /**
//     * AGG Function defined as a accumulator of 2 feature vectors
//     * @param v1
//     * @param v2
//     * @return
//     */
//    public Integer AGG(Integer v1, Integer v2){
//        return v1+v2;
//
//    }
//
//    /**
//     * Sum aggregates the partial aggregations coming from various parts
//     * @param id
//     * @return
//     */
//    public Integer SUM(String id){
//        Optional<Integer> finalAggregation = gnnQueries.get(id)._4().stream().reduce(this::AGG);
//        if(finalAggregation.isPresent())return finalAggregation.get();
//        return 0;
//    }
//
//    /**
//     * GNN update function.
//     * @param agg Aggregation of neighbors
//     * @param e Current vertex
//     * @param l Level of the GNN, L=1 is minimum means that 1-hop aggregation
//     * @return
//     */
//    public void UPD(Integer agg,Vertex e,Short l){
////        Integer res = agg+e.features.get(l-1);
////        e.features.set(l,res);
//    }
//
//    /**
//     * Update the current l-value of vertex from the aggregated neighbor values
//     * @param id
//     * @return
//     */
//    public void runOneCell(String id){
//        Integer agg = SUM(id);
//        Vertex v = this.part.getStorage().getVertex(gnnQueries.get(id)._3());
//        Short l = gnnQueries.get(id)._2();
//        UPD(agg,v,l);
//        gnnQueries.remove(id);
//    }
//
//
//
//
//    /**
//     * Request L level aggregation from in-neighbors
//     * @param v vertex for which agg is done
//     * @param l value of the aggregation, L=1 means aggregation over features of neighbors
//     * @param ctx Context of Flink Stateful
//     */
//    public void reqAggForVertex(Vertex v,short l,Context ctx){
//        assert l>0;
//        // 1. Create a new request in the table
//        String id = UUID.randomUUID().toString();
//        gnnQueries.put(id,new Tuple4<>((short)v.inParts.size(),l,v.getId(),new ArrayList<>()));
//        GNNQuery query = new GNNQuery().withVertex(v).withLValue(l).withOperator(GNNQuery.OPERATORS.AGG_REQ).withId(id);
//        v.inParts.forEach(item->ctx.send(Identifiers.PART_TYPE,item.toString(),query));
//
//    }
//
//    /**
//     * After receiving and aggregation request do a partial aggregation and send the results
//     * @param query Query of type GNNQuery
//     * @// TODO: 01/11/2021 ADD MESSAGE COLLECTION BEFORE AGGREGATION
//     */
//    public Integer aggForVertex(GNNQuery query){
//        Optional<Integer> result = this.part.getStorage()
//                                .getEdges()
//                                .filter(item->item.destination==query.vertex)
//                                .map(item->item.source.getId())
//                                .distinct() // is this line needed. We might need to count double edges twise, no ?!!!
//                                .map(item->this.part.getStorage().getVertex(item))
//                                .map(item->{
////                                    if(item.features.size() < query.L)return 0;
////                                    return item.features.get(query.L - 1);
//                                    return 0;
//                                })
//                                .reduce(this::AGG);
//
//        if(result.isPresent()){
//            return result.get();
//        }
//        return 0;
//    }
//
//    /**
//     * Request all out vertices to update their Hl aggregation value
//     * @param vertex Vertex that is requesting it
//     * @param l l value to be changed
//     * @param ctx
//     */
//    public void reqAggForOutVertices(Vertex vertex, short l,Context ctx){
//
//    }
//
//
//
//
//    @Override
//    public boolean shouldTrigger(Object e) {
//        Class originalClass = e.getClass();
//        for(var i=0;i<acceptedTypes.length;i++){
//            if(originalClass.equals(acceptedTypes[i]))return true;
//        }
//        return false;
//    }
//
//    /**
//     * GNN Query type meassage handler
//     * @param ctx
//     * @param query
//     */
//    public void handleGNNQuery(Context ctx, GNNQuery query){
//        switch (query.op){
//            case NONE -> {
//                System.out.println("Weird operator");
//            }
//            case AGG_REQ -> {
//                query.agg = aggForVertex(query);
//                query.op = GNNQuery.OPERATORS.AGG_RES;
//                ctx.reply(query);
//            }
//            case AGG_RES -> {
//                Tuple4<Short,Short,String,ArrayList<Integer>> tmp = gnnQueries.get(query.uuid);
//                tmp._4().add(query.agg);
//                if(tmp._4().size()== tmp._1()){
//                    runOneCell(query.uuid);
//                }
//            }
//            default ->{
//
//            }
//        }
//    }
//
//    /**
//     * UserQuery Type Message handler
//     * @param ctx
//     * @param query
//     */
//    public void handleUserQuery(Context ctx, UserQuery query){
//        boolean isVertex = query.element instanceof Vertex;
//        boolean isEdge = query.element instanceof Edge;
//        if(!isVertex && !isEdge)return;
//        switch (query.op){
//            case ADD -> {
//                if(isVertex)return;
//                if(isEdge){
//                    // Aggregate the destination vertex if
//                    Vertex dest = ((Edge) query.element).destination;
//                    if(!dest.isMaster())return;
//                    reqAggForVertex((this.part.getStorage().getVertex(dest.getId())),(short)1,ctx);
//                }
//
//            }
//            default -> {
//
//            }
//        }
//
//    }
//
//    /**
//     * General dispatch function, all messages first come here
//     * @param ctx Flink Streaming Context
//     * @param msg Object instance
//     */
//    @Override
//    public void dispatch(Context ctx, Object msg) {
//        // Simply type check -> Cast -> Dispatch to corresponding functions
//        if(msg instanceof UserQuery)handleUserQuery(ctx,(UserQuery) msg);
//        if(msg instanceof GNNQuery)handleGNNQuery(ctx,(GNNQuery) msg);
//        if(msg instanceof BaseReplicatedVertex)handleGNNQuery(ctx,(GNNQuery) msg);
//    }
//}
//
