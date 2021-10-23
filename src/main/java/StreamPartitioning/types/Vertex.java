package StreamPartitioning.types;

/**
 * Vertex of graph that is also a GraphElement
 */
public class Vertex implements GraphElement {
    private String id;

    public Vertex(){
        this.id = null;
    }

    public Vertex withId(String id){
        this.id = id ;
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