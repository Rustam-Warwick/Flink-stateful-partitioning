package StreamPartitioning.aggregators.GNNAggregator;

import StreamPartitioning.aggregators.BaseAggregator;
import StreamPartitioning.edges.Edge;
import StreamPartitioning.types.GraphQuery;
import StreamPartitioning.vertex.BaseReplicatedVertex;
import org.apache.flink.statefun.sdk.Context;
import scala.Int;
import scala.Option;
import scala.Tuple4;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Stream;

abstract public class BaseGNNAggregator<VT extends BaseReplicatedVertex> extends BaseAggregator {
    /**
     * UUID -> (Expected input messages(#in-parts),l-value,vertex id, Arrived data)
     */
    public HashMap<String, Tuple4<Short, Short, String, ArrayList<Object>>> gnnQueries = new HashMap<>();

    public CompletableFuture<Object> AGG(CompletableFuture<Object> f1,CompletableFuture<Object> f2,short l){
        CompletableFuture<Object>[] features = new CompletableFuture[]{f1, f2};
        return CompletableFuture.allOf(features)
                .thenApply(new Function<Void, Integer>() {
                    @Override
                    public Integer apply(Void unused) {
                        try{
                            Integer res1 = (Integer) f1.join();
                            Integer res2 = (Integer) f2.join();
                            return res1+res2;
                        }catch (Exception e){
                            return new Integer(0);
                        }
                    }
                });
    };
    public CompletableFuture<Object> MESSAGE(Edge<? extends BaseReplicatedVertex> edge,short l){
            Edge<VT> tmp = (Edge) edge;
            CompletableFuture<Object>[] features = new CompletableFuture[]{tmp.source.getFeature(l), tmp.destination.getFeature(l)};
            return CompletableFuture.allOf(features).thenApply(new Function<Void, Integer>() {
                @Override
                public Integer apply(Void unused) {
                    try{
                        Integer f0 = (Integer) features[0].join();
                        Integer f1 = (Integer) features[1].join();
                        return f0+f1;
                    }
                    catch (Exception e){
                        return 0;
                    }
                }
            });
    };
    public CompletableFuture<Object> UPDATE(CompletableFuture<Object> agg,BaseReplicatedVertex vertex,short l){
        return new CompletableFuture<>();
    };
    // main Logic Functions
    // Start the AGG by sending AGG_REQ

    public void startGNNCell(Short l, BaseReplicatedVertex vertex, Context c) {
        assert l > 0;
        String id = UUID.randomUUID().toString();
        vertex.inParts.getValue().whenComplete((inParts,trw)->{
            gnnQueries.put(id, new Tuple4<>((short) inParts.size(), l, vertex.getId(), new ArrayList<>()));
            GNNQuery query = new GNNQuery().withOperator(GNNQuery.OPERATORS.AGG_REQ).withLValue(l).withVertex(vertex.getId());
            vertex.sendThisToInPartsAsync(c, query);
        });
    }
    public void continueGNNCell(Tuple4<Short,Short,String,ArrayList<Object>> state){
       Optional<CompletableFuture<Object>> aggResult = state._4().stream()
               .map(item->{
                    CompletableFuture<Object> tmp = new CompletableFuture<>();
                    tmp.complete(item);
                    return tmp;
                    })
               .reduce((v1,v2)->(this.AGG(v1,v2,state._2())));

        if(aggResult.isPresent()){
            System.out.println(aggResult);
//            BaseReplicatedVertex vertex = this.part.getStorage().getVertex(state._3());
//            Object finalResult = this.UPDATE(aggResult.get(),vertex,state._2());
        }
    }

    public CompletableFuture<Object> aggregateLocalMessages(VT v,short l) {
        Optional<CompletableFuture<Object>> tmp = this.part.getStorage().getEdges()
                .filter(item->{
                    return ((Edge) item).destination == v;
                })
                .map(item->{
                    Edge<VT> edge= (Edge) item;
                    return this.MESSAGE(edge,l);
                })
                .reduce((item1,item2)->{
                    CompletableFuture<Object> t1 = (CompletableFuture)item1;
                    CompletableFuture<Object> t2 = (CompletableFuture)item2;
                    return this.AGG(t1,t2,l);
                });
        return tmp.orElse(null);
    }

    // Query Handlers
    public void handleUserQuery(Context ctx, GraphQuery query) {
        boolean isVertex = query.element instanceof BaseReplicatedVertex;
        boolean isEdge = query.element instanceof Edge;
        if (!isVertex && !isEdge) return;
        switch (query.op) {
            case ADD -> {
                if (isEdge) {
                    Edge tmp = (Edge) query.element;
                    this.startGNNCell((short) 1, this.part.getStorage().getVertex(tmp.destination.getId()), ctx);
                }
            }
        }
    }
    public void handleGNNQuery(Context ctx, GNNQuery query){
        switch (query.op){
            case AGG_REQ -> {
                VT v = (VT) this.part.getStorage().getVertex(query.vertexId);
                this.aggregateLocalMessages(v,query.l).whenComplete((aggPart,exc)->{
                    query.withAggValue(aggPart).withOperator(GNNQuery.OPERATORS.AGG_RES);
                    ctx.reply(query);
                });
            }
            case AGG_RES -> {
                Tuple4<Short,Short,String,ArrayList<Object>> gnnState = gnnQueries.get(query.uuid);
                gnnState._4().add(query.agg);
                if(gnnState._4().size()>=gnnState._1()){
                    // Agg messages are ready
                    this.continueGNNCell(gnnState);
                }
            }
        }
    }
    // Overrides
    @Override
    public boolean shouldTrigger(Object e) {
        return true;
    }

    @Override
    public void dispatch(Context ctx, Object msg) {
        // Simply type check -> Cast -> Dispatch to corresponding functions
        if (msg instanceof GraphQuery) handleUserQuery(ctx, (GraphQuery) msg);
        if(msg instanceof GNNQuery)handleGNNQuery(ctx,(GNNQuery) msg);
    }
}
