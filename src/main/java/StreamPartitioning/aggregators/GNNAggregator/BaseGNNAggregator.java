package StreamPartitioning.aggregators.GNNAggregator;

import StreamPartitioning.aggregators.BaseAggregator;
import StreamPartitioning.edges.Edge;
import StreamPartitioning.types.GraphQuery;
import StreamPartitioning.vertex.BaseReplicatedVertex;
import org.apache.flink.statefun.sdk.Context;
import scala.Tuple4;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Optional;
import java.util.UUID;

abstract public class BaseGNNAggregator extends BaseAggregator {
    /**
     * UUID -> (Expected input messages(#in-parts),l-value,vertex id, Arrived data)
     */
    public HashMap<String, Tuple4<Short, Short, String, ArrayList<Object>>> gnnQueries = new HashMap<>();

    // Abstract functions
    abstract public Object AGG(Object f1,Object f2);
    abstract public Object MESSAGE(Edge edge);
    abstract public Object UPDATE(Object agg,BaseReplicatedVertex vertex);
    // main Logic Functions
    // Start the AGG by sending AGG_REQ
    public void startGNNCell(Short l, BaseReplicatedVertex vertex, Context c) {
        assert l > 0;
        String id = UUID.randomUUID().toString();
//        gnnQueries.put(id, new Tuple4<>((short) vertex.inParts.size(), l, vertex.getId(), new ArrayList<>()));
        GNNQuery query = new GNNQuery().withOperator(GNNQuery.OPERATORS.AGG_REQ).withLValue(l).withVertex(vertex.getId());
        vertex.sendThisToInPartsAsync(c, query);
    }
    public void continueGNNCell(Tuple4<Short,Short,String,ArrayList<Object>> state){
       Object aggResult = state._4().stream().reduce(this::AGG);
       BaseReplicatedVertex vertex = this.part.getStorage().getVertex(state._3());
       Object finalResult = this.UPDATE(aggResult,vertex);

    }
    public Object aggregateLocalMessages(BaseReplicatedVertex v) {
        Optional<Object> res = this.part.getStorage().
                getEdges().
                filter(item->(item.destination==v))
                .map(this::MESSAGE)
                .reduce(this::AGG);
        if(res.isPresent())return res.get();
        return null;
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
                BaseReplicatedVertex v = this.part.getStorage().getVertex(query.vertexId);
                Object res = this.aggregateLocalMessages(v);
                query.withAggValue(res).withOperator(GNNQuery.OPERATORS.AGG_RES);
                ctx.reply(query);
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
