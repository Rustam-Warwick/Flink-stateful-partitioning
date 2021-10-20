package StreamPartitioning.types;

/**
 * Simple weighted Edge implementation
 */
public class Edge implements GraphElement {
    private Vertex source;
    private Vertex destination;
    private Float weight = 1.0f;

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
