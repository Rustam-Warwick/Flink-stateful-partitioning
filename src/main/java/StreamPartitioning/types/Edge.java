package StreamPartitioning.types;

public class Edge {
    Integer source;
    Integer destination;
    Float weight;

    public Edge(Integer source, Integer destination, Float weight) {
        this.source = source;
        this.destination = destination;
        this.weight = weight;
    }
}
