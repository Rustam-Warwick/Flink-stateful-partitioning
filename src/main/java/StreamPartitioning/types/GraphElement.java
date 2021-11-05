package StreamPartitioning.types;


import java.util.concurrent.CompletableFuture;

/**
 * GraphElement is either a node or edge, or maybe something else in future
 * Has to implement getId(); which should be unique for all memebers of the instance type
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
    public boolean equals(GraphElement e){
        return e.getClass().equals(this.getClass()) && this.getId()==e.getId();
    }

}
