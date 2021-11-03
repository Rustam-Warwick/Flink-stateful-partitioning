package StreamPartitioning.types;

import StreamPartitioning.vertex.BaseReplicatedVertex;
import StreamPartitioning.vertex.Vertex;

/**
 * Simple weighted Edge implementation
 * Soure,Dest -> (Vertex Id, Part Id)
 */
public class Edge<VT extends BaseReplicatedVertex> implements GraphElement {
    public VT source = null;
    public VT destination = null;
    public Float weight = 1.0f;

    @Override
    public Short getPart() {
        return null;
    }

    @Override
    public Object getFeature(Short l) {
        return 0;
    }

    @Override
    public String getId() {
        return this.source.getId();
    }

    @Override
    public boolean equals(GraphElement e) {
        return getId()==e.getId();
    }


    public Edge betweenVertices(VT source, VT destination) {
        this.source = source;
        this.destination = destination;
        return this;
    }
    public Edge withWeight(Float weight) {
        this.weight = weight;
        return this;
    }
}
