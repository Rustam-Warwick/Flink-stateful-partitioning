package StreamPartitioning.types;

import scala.Tuple2;

/**
 * Simple weighted Edge implementation
 * Soure,Dest -> (Vertex Id, Part Id)
 */
public class Edge implements GraphElement {
    public Vertex source = null;
    public Vertex destination = null;

    public Float weight = 1.0f;
    public Integer inPart = null;


    @Override
    public String getId() {
        return this.source.getId();
    }

    @Override
    public boolean equals(GraphElement e) {
        return getId()==e.getId();
    }


    public Edge betweenVertices(Vertex source, Vertex destination) {
        this.source = source;
        this.destination = destination;
        return this;
    }
    public Edge withWeight(Float weight) {
        this.weight = weight;
        return this;
    }
}
