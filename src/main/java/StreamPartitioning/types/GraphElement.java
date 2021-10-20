package StreamPartitioning.types;


/**
 * GraphElement is either a node or edge, or maybe something else in future
 * Has to implement getId(); which should be unique for all memebers of the instance type
 */
public interface GraphElement {

    public String getId();

}
