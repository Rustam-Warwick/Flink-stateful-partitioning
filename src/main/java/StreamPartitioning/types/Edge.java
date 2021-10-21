package StreamPartitioning.types;

/**
 * Simple weighted Edge implementation
 */
public class Edge implements GraphElement {
    public Vertex source;
    public Vertex destination;
    public Float weight = 1.0f;
    public Edge(){
        this.source = null;
        this.destination = null;
    }

    @Override
    public String getId() {
        return this.source.getId();
    }

    public Edge betweenVertices(Vertex source,Vertex destination) {
        this.source = source;
        this.destination = destination;
        return this;
    }
    public Edge withWeight(Float weight) {
        this.weight = weight;
        return this;
    }
}
