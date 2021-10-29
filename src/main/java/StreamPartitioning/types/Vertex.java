package StreamPartitioning.types;

import java.util.ArrayList;

/**
 * Vertex of graph that is also a GraphElement
 */
public class Vertex implements GraphElement {
    public String id = null;

    public ArrayList<Short> inParts = new ArrayList<>(); // parts with in Neighbors
    public ArrayList<Short> outParts = new ArrayList<>(); // parts with out neighbors

    public Vertex withId(String id){
        this.id = id;
        return this;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public boolean equals(GraphElement e) {
        return getId()==e.getId();
    }

}