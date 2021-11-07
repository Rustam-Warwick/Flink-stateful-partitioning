package StreamPartitioning.types;


import java.util.concurrent.CompletableFuture;

/**
 * Base class for all elements is the graph (Vertex,Edge,)
 */
abstract public class GraphElement{
    public String id = null;
    public Short part = null;
    public GraphElement(String id, Short part){
        this.id = id;
        this.part=part;
    }
    public GraphElement(String id){
        this.id = id;
        this.part = null;
    }
    public GraphElement(){
        this.id = null;
        this.part = null;
    }

    public String getId(){
        return this.id;
    }
    public void setId(String id){
        this.id = id;
    }
    public void setPart(Short part){
        this.part = part;
    }
    public Short getPart(){
        return this.part;
    }
    @Override
    public boolean equals(Object o){
        GraphElement e =(GraphElement) o;
        return e.getClass().toString().equals(this.getClass().toString()) && this.getId().equals(e.getId());
    }

}
