package StreamPartitioning.types;

import StreamPartitioning.vertex.BaseReplicatedVertex;
import StreamPartitioning.vertex.Vertex;
import org.apache.flink.statefun.sdk.Context;

import java.util.concurrent.CompletableFuture;

/**
 * Simple weighted Edge implementation
 * Soure,Dest -> (Vertex Id, Part Id)
 */
public class Edge extends GraphElement {
    public BaseReplicatedVertex source = null;
    public BaseReplicatedVertex destination = null;
    public Float weight = 1.0f;
    public Edge(String id, Short partId){
        super(id,partId);
    }
    public Edge(String id ){
        super(id);
    }
    public Edge(BaseReplicatedVertex source,BaseReplicatedVertex destination){
        super(source.getId()+destination.getId());
        this.source = source;
        this.destination = destination;
    }

    public Edge withWeight(Float weight) {
        this.weight = weight;
        return this;
    }
}
