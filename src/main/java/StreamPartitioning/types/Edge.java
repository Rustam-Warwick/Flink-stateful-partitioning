package StreamPartitioning.types;

import scala.Tuple2;

/**
 * Simple weighted Edge implementation
 * Soure,Dest -> (Vertex Id, Part Id)
 */
public class Edge implements GraphElement {
    public Tuple2<String,Integer> source = null;
    public Tuple2<String,Integer> destination = null;
    public Float weight = 1.0f;
    public Integer inPart = null;


    @Override
    public String getId() {
        return this.source._1;
    }

    @Override
    public boolean equals(GraphElement e) {
        return getId()==e.getId();
    }

    @Override
    public Integer getPartId() {
        return inPart;
    }

    public Edge betweenVertices(Tuple2<String,Integer> source, Tuple2<String,Integer> destination) {
        this.source = source;
        this.destination = destination;
        return this;
    }
    public Edge withWeight(Float weight) {
        this.weight = weight;
        return this;
    }
}
