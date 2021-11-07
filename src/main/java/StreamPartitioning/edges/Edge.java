package StreamPartitioning.edges;

import StreamPartitioning.types.GraphElement;
import StreamPartitioning.vertex.BaseReplicatedVertex;
import StreamPartitioning.vertex.BaseVertex;

/**
 * Simple weighted Edge implementation
 * Soure,Dest -> (Vertex Id, Part Id)
 */
public class Edge<T extends BaseReplicatedVertex> extends GraphElement {
    public T source = null;
    public T destination = null;
    public Float weight = 1.0f;
    public Edge(String id, Short partId){
        super(id,partId);
    }
    public Edge(String id ){
        super(id);
    }
    public Edge(T source,T destination){
        super(source.getId()+destination.getId());
        this.source = source;
        this.destination = destination;
    }

    public Edge<T> withWeight(Float weight) {
        this.weight = weight;
        return this;
    }
}
