package StreamPartitioning.types;

/**
 * Vertex of graph that is also a GraphElement
 */
public class Vertex implements GraphElement {
    private String id = null;
    private Integer inPart = null;


    public Vertex withId(String id){
        this.id = id;
        return this;
    }

    public Vertex inPart(Integer id){
        this.inPart = id;
        return this;
    }

    @Override
    public Integer getPartId() {
        return inPart;
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