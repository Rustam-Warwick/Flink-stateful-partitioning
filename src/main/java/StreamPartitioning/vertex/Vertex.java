package StreamPartitioning.vertex;

import StreamPartitioning.aggregators.GNNAggregator.Aggregatable;
import StreamPartitioning.features.ReplicableIntegerFeature;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Vertex of graph that is also a GraphElement
 */
public class Vertex extends BaseReplicatedVertex{
//    public ReplicableIntegerFeature feature = new ReplicableIntegerFeature("feature",this);
//    public ReplicableIntegerFeature agg_1 = new ReplicableIntegerFeature("agg_1",this);
//    public ReplicableIntegerFeature agg_2 = new ReplicableIntegerFeature("agg_2",this);

    @Override
    public CompletableFuture<Integer> getFeature(short l) {
        return new CompletableFuture();
//        switch (l){
//            case 0:
//                return feature.getValue().thenApply(item->item[0]);
//            case 1:
//                return agg_1.getValue().thenApply(item->item[0]);
//            case 2:
//                return agg_2.getValue().thenApply(item->item[0]);
//            default:
//            {
//                CompletableFuture<Integer> tmp = new CompletableFuture<>();
//                tmp.complete(new Integer(0));
//                return tmp;
//            }
//
//        }
    }
    public Vertex(){
        super();
    }
    public Vertex(String id){
        super(id);
    }
    public Vertex(String id, Short part){
        super(id,part);
    }
    public Vertex(String id, Short part,Short masterPart){
        super(id,part,masterPart);
    }

    @Override
    public BaseReplicatedVertex copy() {
        return new Vertex(this.id,this.part,this.masterPart);
    }
}